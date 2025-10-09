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

#include "json-test.h"
#include "test-libmongoc.h"

#include "bson-dsl.h"

#include "common-b64-private.h"

/* _mongoc_host_list_from_string_with_err */
#include "mongoc/mongoc-host-list-private.h"
#include "mongoc/mongoc-cluster-aws-private.h"

/* MONGOC_SERVER_ERR_NS_NOT_FOUND */
#include "mongoc/mongoc-error-private.h"

#include "mongoc/mongoc-client-side-encryption-private.h"

#include "mongoc/mongoc-uri.h"

static void
_before_test (json_test_ctx_t *ctx, const bson_t *test)
{
   mongoc_client_t *client;
   mongoc_collection_t *keyvault_coll;
   bson_iter_t iter;
   bson_error_t error;
   bool ret;
   mongoc_write_concern_t *wc;
   bson_t insert_opts;

   BSON_UNUSED (test);

   /* Insert data into the key vault. */
   client = test_framework_new_default_client ();
   wc = mongoc_write_concern_new ();
   mongoc_write_concern_set_wmajority (wc, 1000);
   bson_init (&insert_opts);
   mongoc_write_concern_append (wc, &insert_opts);

   if (bson_iter_init_find (&iter, ctx->config->scenario, "key_vault_data")) {
      keyvault_coll = mongoc_client_get_collection (client, "keyvault", "datakeys");

      /* Drop and recreate, inserting data. */
      ret = mongoc_collection_drop (keyvault_coll, &error);
      if (!ret) {
         /* Ignore "namespace does not exist" error. */
         ASSERT_OR_PRINT (error.code == 26, error);
      }

      bson_iter_recurse (&iter, &iter);
      while (bson_iter_next (&iter)) {
         bson_t doc;

         bson_iter_bson (&iter, &doc);
         ret = mongoc_collection_insert_one (keyvault_coll, &doc, &insert_opts, NULL /* reply */, &error);
         ASSERT_OR_PRINT (ret, error);
      }
      mongoc_collection_destroy (keyvault_coll);
   }

   bson_destroy (&insert_opts);
   mongoc_write_concern_destroy (wc);
   mongoc_client_destroy (client);
}

static bool
_run_operation (json_test_ctx_t *ctx, const bson_t *test, const bson_t *operation)
{
   bson_t reply;
   bool res;

   res = json_test_operation (ctx, test, operation, ctx->collection, NULL, &reply);

   bson_destroy (&reply);

   return res;
}

static void
test_client_side_encryption_cb (bson_t *scenario)
{
   json_test_config_t config = JSON_TEST_CONFIG_INIT;
   config.before_test_cb = _before_test;
   config.run_operation_cb = _run_operation;
   config.scenario = scenario;
   config.command_started_events_only = true;
   config.command_monitoring_allow_subset = false;
   run_json_general_test (&config);
}

/* This is the hex form of the base64 encoded value:
 * Mng0NCt4ZHVUYUJCa1kxNkVyNUR1QURhZ2h2UzR2d2RrZzh0cFBwM3R6NmdWMDFBMUN3YkQ5aXRRMkhGRGdQV09wOGVNYUMxT2k3NjZKelhaQmRCZGJkTXVyZG9uSjFk
 * From the client side encryption spec.
 */
#define LOCAL_MASTERKEY                                                       \
   "\x32\x78\x34\x34\x2b\x78\x64\x75\x54\x61\x42\x42\x6b\x59\x31\x36\x45\x72" \
   "\x35\x44\x75\x41\x44\x61\x67\x68\x76\x53\x34\x76\x77\x64\x6b\x67\x38\x74" \
   "\x70\x50\x70\x33\x74\x7a\x36\x67\x56\x30\x31\x41\x31\x43\x77\x62\x44\x39" \
   "\x69\x74\x51\x32\x48\x46\x44\x67\x50\x57\x4f\x70\x38\x65\x4d\x61\x43\x31" \
   "\x4f\x69\x37\x36\x36\x4a\x7a\x58\x5a\x42\x64\x42\x64\x62\x64\x4d\x75\x72" \
   "\x64\x6f\x6e\x4a\x31\x64"

static void
_set_extra_bypass (bson_t *extra)
{
   if (test_framework_getenv_bool ("MONGOC_TEST_MONGOCRYPTD_BYPASS_SPAWN")) {
      BSON_APPEND_BOOL (extra, "mongocryptdBypassSpawn", true);
   }
}

static void
_set_extra_crypt_shared (bson_t *extra)
{
   char *const path = test_framework_getenv ("MONGOC_TEST_CRYPT_SHARED_LIB_PATH");
   if (path) {
      BSON_APPEND_UTF8 (extra, "cryptSharedLibPath", path);
      bson_free (path);
   }
}

/* Convenience helper to check if spawning mongocryptd should be bypassed */
static void
_check_bypass (mongoc_auto_encryption_opts_t *opts)
{
   bson_t extra = BSON_INITIALIZER;
   _set_extra_bypass (&extra);
   mongoc_auto_encryption_opts_set_extra (opts, &extra);
   bson_destroy (&extra);
}

static bson_t *
_make_aws_kms_provider (bson_t *kms_providers)
{
   char *aws_secret_access_key;
   char *aws_access_key_id;

   aws_secret_access_key = test_framework_getenv_required ("MONGOC_TEST_AWS_SECRET_ACCESS_KEY");
   aws_access_key_id = test_framework_getenv_required ("MONGOC_TEST_AWS_ACCESS_KEY_ID");

   if (!kms_providers) {
      kms_providers = bson_new ();
   }

   bson_concat (kms_providers,
                tmp_bson ("{ 'aws': { 'secretAccessKey': '%s', 'accessKeyId': '%s' }}",
                          aws_secret_access_key,
                          aws_access_key_id));

   bson_free (aws_secret_access_key);
   bson_free (aws_access_key_id);

   return kms_providers;
}

static bson_t *
_make_azure_kms_provider (bson_t *kms_providers)
{
   char *azure_tenant_id;
   char *azure_client_id;
   char *azure_client_secret;

   azure_tenant_id = test_framework_getenv_required ("MONGOC_TEST_AZURE_TENANT_ID");
   azure_client_id = test_framework_getenv_required ("MONGOC_TEST_AZURE_CLIENT_ID");
   azure_client_secret = test_framework_getenv_required ("MONGOC_TEST_AZURE_CLIENT_SECRET");

   if (!kms_providers) {
      kms_providers = bson_new ();
   }

   bson_concat (kms_providers,
                tmp_bson ("{ 'azure': { 'tenantId': '%s', 'clientId': '%s', "
                          "'clientSecret': '%s' }}",
                          azure_tenant_id,
                          azure_client_id,
                          azure_client_secret));

   bson_free (azure_tenant_id);
   bson_free (azure_client_id);
   bson_free (azure_client_secret);

   return kms_providers;
}

static bson_t *
_make_gcp_kms_provider (bson_t *kms_providers)
{
   char *gcp_email;
   char *gcp_privatekey;


   gcp_email = test_framework_getenv_required ("MONGOC_TEST_GCP_EMAIL");
   gcp_privatekey = test_framework_getenv_required ("MONGOC_TEST_GCP_PRIVATEKEY");

   if (!gcp_email || !gcp_privatekey) {
      test_error ("Set MONGOC_TEST_GCP_EMAIL and MONGOC_TEST_GCP_PRIVATEKEY to "
                  "enable CSFLE tests.");
   }

   if (!kms_providers) {
      kms_providers = bson_new ();
   }

   bson_concat (kms_providers, tmp_bson ("{ 'gcp': { 'email': '%s', 'privateKey': '%s' }}", gcp_email, gcp_privatekey));

   bson_free (gcp_email);
   bson_free (gcp_privatekey);

   return kms_providers;
}

static bson_t *
_make_local_kms_provider (bson_t *kms_providers)
{
   bson_t *local = BCON_NEW ("local", "{", "key", BCON_BIN (0, (uint8_t *) LOCAL_MASTERKEY, 96), "}");

   if (!kms_providers) {
      kms_providers = bson_new ();
   }

   bson_concat (kms_providers, local);
   bson_destroy (local);

   return kms_providers;
}

static bson_t *
_make_kmip_kms_provider (bson_t *kms_providers)
{
   if (!kms_providers) {
      kms_providers = bson_new ();
   }

   bson_concat (kms_providers, tmp_bson ("{ 'kmip': { 'endpoint': 'localhost:5698' } }"));


   return kms_providers;
}

/* Convenience helper for creating KMS providers doc */
static bson_t *
_make_kms_providers (bool with_aws, bool with_local)
{
   bson_t *kms_providers = bson_new ();

   if (with_aws) {
      _make_aws_kms_provider (kms_providers);
      _make_azure_kms_provider (kms_providers);
      _make_gcp_kms_provider (kms_providers);
      _make_kmip_kms_provider (kms_providers);
   }

   if (with_local) {
      _make_local_kms_provider (kms_providers);
   }

   return kms_providers;
}

static bson_t *
_make_tls_opts (void)
{
   bson_t *tls_opts = bson_new ();
   char *kmip_tls_ca_file;
   char *kmip_tls_certificate_key_file;

   kmip_tls_ca_file = test_framework_getenv_required ("MONGOC_TEST_CSFLE_TLS_CA_FILE");
   kmip_tls_certificate_key_file = test_framework_getenv_required ("MONGOC_TEST_CSFLE_TLS_CERTIFICATE_KEY_FILE");

   bson_concat (tls_opts,
                tmp_bson ("{ 'kmip': {  'tlsCAFile': '%s', 'tlsCertificateKeyFile': '%s' } }",
                          kmip_tls_ca_file,
                          kmip_tls_certificate_key_file));

   bson_free (kmip_tls_ca_file);
   bson_free (kmip_tls_certificate_key_file);
   return tls_opts;
}

static bson_t *
_make_kms_masterkey (char const *provider)
{
   if (strcmp (provider, "aws") == 0) {
      return BCON_NEW ("region",
                       "us-east-1",
                       "key",
                       "arn:aws:kms:us-east-1:579766882180:key/"
                       "89fcc2c4-08b0-4bd9-9f25-e30687b580d0");
   }

   if (strcmp (provider, "azure") == 0) {
      return BCON_NEW ("keyVaultEndpoint", "key-vault-csfle.vault.azure.net", "keyName", "key-name-csfle");
   }

   if (strcmp (provider, "gcp") == 0) {
      return BCON_NEW ("projectId",
                       "devprod-drivers",
                       "location",
                       "global",
                       "keyRing",
                       "key-ring-csfle",
                       "keyName",
                       "key-name-csfle");
   }

   if (strcmp (provider, "kmip") == 0) {
      return bson_new ();
   }

   if (strcmp (provider, "local") == 0) {
      return bson_new ();
   }

   return NULL;
}

typedef struct {
   int num_inserts;
} limits_apm_ctx_t;

static void
_command_started (const mongoc_apm_command_started_t *event)
{
   limits_apm_ctx_t *ctx;

   ctx = (limits_apm_ctx_t *) mongoc_apm_command_started_get_context (event);
   if (0 == strcmp ("insert", mongoc_apm_command_started_get_command_name (event))) {
      ctx->num_inserts++;
   }
}

/* Prose Test 4: BSON Size Limits and Batch Splitting */
static void
test_bson_size_limits_and_batch_splitting (void *unused)
{
   /* Expect an insert of two documents over 2MiB to split into two inserts but
    * still succeed. */
   mongoc_client_t *client;
   mongoc_auto_encryption_opts_t *opts;
   mongoc_uri_t *uri;
   mongoc_collection_t *coll;
   bson_error_t error;
   bson_t *corpus_schema;
   bson_t *datakey;
   bson_t *cmd;
   bson_t *kms_providers;
   bson_t *docs[2];
   char *as;
   limits_apm_ctx_t ctx = {0};
   mongoc_apm_callbacks_t *callbacks;
   /* Values from the spec to test boundaries. */
   const int size_16mib = 16777216;
   const int size_2mib = 2097152;
   const int exceeds_2mib_after_encryption = size_2mib - 2000;
   const int exceeds_16mib_after_encryption = size_16mib - 2000;

   BSON_UNUSED (unused);

   /* Do the test setup. */

   /* Drop and create db.coll configured with limits-schema.json */
   uri = test_framework_get_uri ();
   client = test_framework_client_new_from_uri (uri, NULL);
   test_framework_set_ssl_opts (client);
   mongoc_client_set_error_api (client, MONGOC_ERROR_API_VERSION_2);
   coll = mongoc_client_get_collection (client, "db", "coll");
   (void) mongoc_collection_drop (coll, NULL);
   corpus_schema = get_bson_from_json_file ("./src/libmongoc/tests/client_side_encryption_prose/limits-schema.json");
   cmd = BCON_NEW ("create", "coll", "validator", "{", "$jsonSchema", BCON_DOCUMENT (corpus_schema), "}");
   ASSERT_OR_PRINT (mongoc_client_command_simple (client, "db", cmd, NULL /* read prefs */, NULL /* reply */, &error),
                    error);

   mongoc_collection_destroy (coll);
   /* Drop and create the key vault collection, keyvault.datakeys. */
   {
      mongoc_write_concern_t *wc;

      coll = mongoc_client_get_collection (client, "keyvault", "datakeys");
      (void) mongoc_collection_drop (coll, NULL);
      datakey = get_bson_from_json_file ("./src/libmongoc/tests/client_side_encryption_prose/limits-key.json");
      wc = mongoc_write_concern_new ();
      mongoc_write_concern_set_wmajority (wc, 1000);
      mongoc_collection_set_write_concern (coll, wc);
      ASSERT_OR_PRINT (mongoc_collection_insert_one (coll, datakey, NULL /* opts */, NULL /* reply */, &error), error);
      mongoc_write_concern_destroy (wc);
      mongoc_collection_destroy (coll);
   }

   mongoc_client_destroy (client);

   client = test_framework_client_new_from_uri (uri, NULL);
   test_framework_set_ssl_opts (client);
   mongoc_client_set_error_api (client, MONGOC_ERROR_API_VERSION_2);

   kms_providers = _make_kms_providers (false /* aws */, true /* local */);
   opts = mongoc_auto_encryption_opts_new ();
   _check_bypass (opts);
   mongoc_auto_encryption_opts_set_keyvault_namespace (opts, "keyvault", "datakeys");
   mongoc_auto_encryption_opts_set_kms_providers (opts, kms_providers);

   ASSERT_OR_PRINT (mongoc_client_enable_auto_encryption (client, opts, &error), error);

   callbacks = mongoc_apm_callbacks_new ();
   mongoc_apm_set_command_started_cb (callbacks, _command_started);
   mongoc_client_set_apm_callbacks (client, callbacks, &ctx);

   coll = mongoc_client_get_collection (client, "db", "coll");
   /* End of setup */

   /* Insert { "_id": "over_2mib_under_16mib", "unencrypted": <the string "a"
    * repeated 2097152 times> } */
   docs[0] = BCON_NEW ("_id", "over_2mib_under_16mib");
   as = bson_malloc (size_16mib);
   memset (as, 'a', size_16mib);
   bson_append_utf8 (docs[0], "unencrypted", -1, as, 2097152);
   ASSERT_OR_PRINT (mongoc_collection_insert_one (coll, docs[0], NULL /* opts */, NULL /* reply */, &error), error);
   bson_destroy (docs[0]);

   /* Insert the document `limits/limits-doc.json <../limits/limits-doc.json>`_
    * concatenated with ``{ "_id": "encryption_exceeds_2mib", "unencrypted": <
    * the string "a" repeated (2097152 - 2000) times > }`` */
   docs[0] = get_bson_from_json_file ("./src/libmongoc/tests/client_side_encryption_prose/limits-doc.json");
   bson_append_utf8 (docs[0], "_id", -1, "encryption_exceeds_2mib", -1);
   bson_append_utf8 (docs[0], "unencrypted", -1, as, exceeds_2mib_after_encryption);
   ASSERT_OR_PRINT (mongoc_collection_insert_one (coll, docs[0], NULL /* opts */, NULL /* reply */, &error), error);
   bson_destroy (docs[0]);

   /* Insert two documents that each exceed 2MiB but no encryption occurs.
    * Expect the bulk write to succeed and run as two separate inserts.
    */
   docs[0] = BCON_NEW ("_id", "over_2mib_1");
   bson_append_utf8 (docs[0], "unencrypted", -1, as, size_2mib);
   docs[1] = BCON_NEW ("_id", "over_2mib_2");
   bson_append_utf8 (docs[1], "unencrypted", -1, as, size_2mib);
   ctx.num_inserts = 0;
   ASSERT_OR_PRINT (
      mongoc_collection_insert_many (coll, (const bson_t **) docs, 2, NULL /* opts */, NULL /* reply */, &error),
      error);
   ASSERT_CMPINT (ctx.num_inserts, ==, 2);
   bson_destroy (docs[0]);
   bson_destroy (docs[1]);

   /* Insert two documents that each exceed 2MiB after encryption occurs. Expect
    * the bulk write to succeed and run as two separate inserts.
    */

   docs[0] = get_bson_from_json_file ("./src/libmongoc/tests/client_side_encryption_prose/limits-doc.json");
   bson_append_utf8 (docs[0], "_id", -1, "encryption_exceeds_2mib_1", -1);
   bson_append_utf8 (docs[0], "unencrypted", -1, as, exceeds_2mib_after_encryption);
   docs[1] = get_bson_from_json_file ("./src/libmongoc/tests/client_side_encryption_prose/limits-doc.json");
   bson_append_utf8 (docs[1], "_id", -1, "encryption_exceeds_2mib_2", -1);
   bson_append_utf8 (docs[1], "unencrypted", -1, as, exceeds_2mib_after_encryption);
   ctx.num_inserts = 0;
   ASSERT_OR_PRINT (
      mongoc_collection_insert_many (coll, (const bson_t **) docs, 2, NULL /* opts */, NULL /* reply */, &error),
      error);
   ASSERT_CMPINT (ctx.num_inserts, ==, 2);
   bson_destroy (docs[0]);
   bson_destroy (docs[1]);

   /* Check that inserting close to, but not exceeding, 16MiB, passes */
   docs[0] = bson_new ();
   bson_append_utf8 (docs[0], "_id", -1, "under_16mib", -1);
   bson_append_utf8 (docs[0], "unencrypted", -1, as, exceeds_16mib_after_encryption);
   ASSERT_OR_PRINT (mongoc_collection_insert_one (coll, docs[0], NULL /* opts */, NULL /* reply */, &error), error);
   bson_destroy (docs[0]);

   /* but.. exceeding 16 MiB fails */
   docs[0] = get_bson_from_json_file ("./src/libmongoc/tests/client_side_encryption_prose/limits-doc.json");
   bson_append_utf8 (docs[0], "_id", -1, "under_16mib", -1);
   bson_append_utf8 (docs[0], "unencrypted", -1, as, exceeds_16mib_after_encryption);
   BSON_ASSERT (!mongoc_collection_insert_one (coll, docs[0], NULL /* opts */, NULL /* reply */, &error));
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_SERVER, 2, "too large");
   bson_destroy (docs[0]);

   bson_free (as);
   bson_destroy (kms_providers);
   bson_destroy (corpus_schema);
   bson_destroy (cmd);
   bson_destroy (datakey);
   mongoc_collection_destroy (coll);
   mongoc_client_destroy (client);
   mongoc_uri_destroy (uri);
   mongoc_apm_callbacks_destroy (callbacks);
   mongoc_auto_encryption_opts_destroy (opts);
}

typedef struct {
   bson_t *last_cmd;
} _datakey_and_double_encryption_ctx_t;

static void
_datakey_and_double_encryption_command_started (const mongoc_apm_command_started_t *event)
{
   _datakey_and_double_encryption_ctx_t *ctx;

   ctx = (_datakey_and_double_encryption_ctx_t *) mongoc_apm_command_started_get_context (event);
   bson_destroy (ctx->last_cmd);
   ctx->last_cmd = bson_copy (mongoc_apm_command_started_get_command (event));
}

static void
test_datakey_and_double_encryption_creating_and_using (mongoc_client_encryption_t *client_encryption,
                                                       mongoc_client_t *client,
                                                       mongoc_client_t *client_encrypted,
                                                       const char *kms_provider,
                                                       _datakey_and_double_encryption_ctx_t *test_ctx)
{
   bson_value_t keyid;
   bson_error_t error;
   bool ret;
   mongoc_client_encryption_datakey_opts_t *opts;
   mongoc_client_encryption_encrypt_opts_t *encrypt_opts;
   char *altname;
   mongoc_collection_t *coll;
   mongoc_cursor_t *cursor;
   bson_t filter;
   const bson_t *doc;
   bson_value_t to_encrypt;
   bson_value_t encrypted;
   bson_value_t encrypted_via_altname;
   bson_t to_insert = BSON_INITIALIZER;
   char *hello;

   opts = mongoc_client_encryption_datakey_opts_new ();

   if (0 == strcmp (kms_provider, "aws")) {
      mongoc_client_encryption_datakey_opts_set_masterkey (opts,
                                                           tmp_bson ("{ 'region': 'us-east-1', 'key': "
                                                                     "'arn:aws:kms:us-east-1:579766882180:key/"
                                                                     "89fcc2c4-08b0-4bd9-9f25-e30687b580d0' }"));
   } else if (0 == strcmp (kms_provider, "azure")) {
      mongoc_client_encryption_datakey_opts_set_masterkey (
         opts,
         tmp_bson ("{'keyVaultEndpoint': 'key-vault-csfle.vault.azure.net', "
                   "'keyName': 'key-name-csfle'}"));
   } else if (0 == strcmp (kms_provider, "gcp")) {
      mongoc_client_encryption_datakey_opts_set_masterkey (opts,
                                                           tmp_bson ("{'projectId': 'devprod-drivers','location': "
                                                                     "'global','keyRing': 'key-ring-csfle','keyName': "
                                                                     "'key-name-csfle'}"));
   } else if (0 == strcmp (kms_provider, "kmip")) {
      mongoc_client_encryption_datakey_opts_set_masterkey (opts, tmp_bson ("{}"));
   }

   altname = bson_strdup_printf ("%s_altname", kms_provider);
   mongoc_client_encryption_datakey_opts_set_keyaltnames (opts, &altname, 1);

   ret = mongoc_client_encryption_create_datakey (client_encryption, kms_provider, opts, &keyid, &error);
   ASSERT_OR_PRINT (ret, error);

   /* Expect a BSON binary with subtype 4 to be returned */
   BSON_ASSERT (keyid.value_type == BSON_TYPE_BINARY);
   BSON_ASSERT (keyid.value.v_binary.subtype == BSON_SUBTYPE_UUID);

   /* Check that client captured a command_started event for the insert command
    * containing a majority writeConcern. */
   assert_match_bson (
      test_ctx->last_cmd, tmp_bson ("{'insert': 'datakeys', 'writeConcern': { 'w': 'majority' } }"), false);

   /* Use client to run a find on keyvault.datakeys */
   coll = mongoc_client_get_collection (client, "keyvault", "datakeys");
   bson_init (&filter);
   BSON_APPEND_VALUE (&filter, "_id", &keyid);
   cursor = mongoc_collection_find_with_opts (coll, &filter, NULL /* opts */, NULL /* read prefs */);
   mongoc_collection_destroy (coll);

   /* Expect that exactly one document is returned with the "masterKey.provider"
    * equal to <kms_provider> */
   BSON_ASSERT (mongoc_cursor_next (cursor, &doc));
   BSON_ASSERT (0 == strcmp (kms_provider, bson_lookup_utf8 (doc, "masterKey.provider")));
   BSON_ASSERT (!mongoc_cursor_next (cursor, &doc));
   ASSERT_OR_PRINT (!mongoc_cursor_error (cursor, &error), error);
   mongoc_cursor_destroy (cursor);

   /* Call client_encryption.encrypt() with the value "hello <kms provider>" */
   encrypt_opts = mongoc_client_encryption_encrypt_opts_new ();
   mongoc_client_encryption_encrypt_opts_set_algorithm (encrypt_opts,
                                                        MONGOC_AEAD_AES_256_CBC_HMAC_SHA_512_DETERMINISTIC);
   mongoc_client_encryption_encrypt_opts_set_keyid (encrypt_opts, &keyid);

   hello = bson_strdup_printf ("hello %s", kms_provider);

   {
      to_encrypt.value_type = BSON_TYPE_UTF8;
      to_encrypt.value.v_utf8.str = bson_strdup (hello);

      const size_t len = strlen (to_encrypt.value.v_utf8.str);
      ASSERT (bson_in_range_unsigned (uint32_t, len));
      to_encrypt.value.v_utf8.len = (uint32_t) len;
   }

   ret = mongoc_client_encryption_encrypt (client_encryption, &to_encrypt, encrypt_opts, &encrypted, &error);
   ASSERT_OR_PRINT (ret, error);
   mongoc_client_encryption_encrypt_opts_destroy (encrypt_opts);

   /* Expect the return value to be a BSON binary subtype 6 */
   BSON_ASSERT (encrypted.value_type == BSON_TYPE_BINARY);
   BSON_ASSERT (encrypted.value.v_binary.subtype == BSON_SUBTYPE_ENCRYPTED);

   /* Use client_encrypted to insert { _id: "<kms provider>", "value":
    * <encrypted> } into db.coll */
   coll = mongoc_client_get_collection (client_encrypted, "db", "coll");
   BSON_APPEND_UTF8 (&to_insert, "_id", kms_provider);
   BSON_APPEND_VALUE (&to_insert, "value", &encrypted);
   ret = mongoc_collection_insert_one (coll, &to_insert, NULL /* opts */, NULL /* reply */, &error);
   ASSERT_OR_PRINT (ret, error);

   /* Use client_encrypted to run a find querying with _id of <kms_provider> and
    * expect value to be "hello <kms_provider>". */
   cursor = mongoc_collection_find_with_opts (
      coll, tmp_bson ("{ '_id': '%s' }", kms_provider), NULL /* opts */, NULL /* read prefs */);
   BSON_ASSERT (mongoc_cursor_next (cursor, &doc));
   BSON_ASSERT (0 == strcmp (hello, bson_lookup_utf8 (doc, "value")));
   BSON_ASSERT (!mongoc_cursor_next (cursor, &doc));
   ASSERT_OR_PRINT (!mongoc_cursor_error (cursor, &error), error);
   mongoc_cursor_destroy (cursor);
   mongoc_collection_destroy (coll);

   /* Call client_encryption.encrypt() with the value "hello <kms provider>",
    * the algorithm AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic, and the
    * key_alt_name of <kms provider>_altname. */
   encrypt_opts = mongoc_client_encryption_encrypt_opts_new ();
   mongoc_client_encryption_encrypt_opts_set_algorithm (encrypt_opts,
                                                        MONGOC_AEAD_AES_256_CBC_HMAC_SHA_512_DETERMINISTIC);
   mongoc_client_encryption_encrypt_opts_set_keyaltname (encrypt_opts, altname);

   ret =
      mongoc_client_encryption_encrypt (client_encryption, &to_encrypt, encrypt_opts, &encrypted_via_altname, &error);
   ASSERT_OR_PRINT (ret, error);
   mongoc_client_encryption_encrypt_opts_destroy (encrypt_opts);

   /* Expect the return value to be a BSON binary subtype 6. Expect the value to
    * exactly match the value of encrypted. */
   BSON_ASSERT (encrypted_via_altname.value_type == BSON_TYPE_BINARY);
   BSON_ASSERT (encrypted_via_altname.value.v_binary.subtype == BSON_SUBTYPE_ENCRYPTED);
   BSON_ASSERT (encrypted_via_altname.value.v_binary.data_len == encrypted.value.v_binary.data_len);
   BSON_ASSERT (0 == memcmp (encrypted_via_altname.value.v_binary.data,
                             encrypted.value.v_binary.data,
                             encrypted.value.v_binary.data_len));

   bson_value_destroy (&encrypted);
   bson_value_destroy (&encrypted_via_altname);
   bson_free (hello);
   bson_destroy (&to_insert);
   bson_value_destroy (&to_encrypt);
   bson_value_destroy (&keyid);
   bson_free (altname);
   bson_destroy (&filter);
   mongoc_client_encryption_datakey_opts_destroy (opts);
}

/* Prose Test 1: Custom Key Material Test */
static void
test_create_datakey_with_custom_key_material (void *unused)
{
   mongoc_client_t *client = NULL;
   mongoc_client_encryption_t *client_encryption = NULL;
   bson_error_t error;
   bson_t datakey = BSON_INITIALIZER;

   BSON_UNUSED (unused);

   /* Create a MongoClient object (referred to as client). */
   client = test_framework_new_default_client ();

   /* Using client, drop the collection keyvault.datakeys. */
   {
      mongoc_collection_t *const datakeys = mongoc_client_get_collection (client, "keyvault", "datakeys");
      (void) mongoc_collection_drop (datakeys, NULL);
      mongoc_collection_destroy (datakeys);
   }

   /* Create a ClientEncryption object (referred to as client_encryption) with
    * client set as the keyVaultClient. */
   {
      mongoc_client_encryption_opts_t *const client_encryption_opts = mongoc_client_encryption_opts_new ();
      bson_t *const kms_providers = _make_kms_providers (true /* aws */, true /* local */);
      bson_t *const tls_opts = _make_tls_opts ();

      mongoc_client_encryption_opts_set_kms_providers (client_encryption_opts, kms_providers);
      mongoc_client_encryption_opts_set_tls_opts (client_encryption_opts, tls_opts);
      mongoc_client_encryption_opts_set_keyvault_namespace (client_encryption_opts, "keyvault", "datakeys");
      mongoc_client_encryption_opts_set_keyvault_client (client_encryption_opts, client);
      client_encryption = mongoc_client_encryption_new (client_encryption_opts, &error);
      ASSERT_OR_PRINT (client_encryption, error);

      mongoc_client_encryption_opts_destroy (client_encryption_opts);
      bson_destroy (kms_providers);
      bson_destroy (tls_opts);
   }

   /* Using client_encryption, create a data key with a local KMS provider and
    * the following custom key material: */
   {
      const char key_material[] = "xPTAjBRG5JiPm+d3fj6XLi2q5DMXUS/"
                                  "f1f+SMAlhhwkhDRL0kr8r9GDLIGTAGlvC+HVjSIgdL+"
                                  "RKwZCvpXSyxTICWSXTUYsWYPyu3IoHbuBZdmw2faM3WhcRIgbMReU5";
      uint8_t data[96];
      mongoc_client_encryption_datakey_opts_t *datakey_opts = mongoc_client_encryption_datakey_opts_new ();
      bson_value_t keyid;

      BSON_ASSERT (mcommon_b64_pton (key_material, data, sizeof (key_material)) == 96);

      mongoc_client_encryption_datakey_opts_set_keymaterial (datakey_opts, data, sizeof (data));

      ASSERT_OR_PRINT (
         mongoc_client_encryption_create_datakey (client_encryption, "local", datakey_opts, &keyid, &error), error);

      ASSERT (keyid.value_type == BSON_TYPE_BINARY);
      ASSERT (keyid.value.v_binary.subtype == BSON_SUBTYPE_UUID);
      ASSERT (keyid.value.v_binary.data_len != 0);

      mongoc_client_encryption_datakey_opts_destroy (datakey_opts);
      bson_value_destroy (&keyid);
   }

   /* Find the resulting key document in keyvault.datakeys, save a copy of the
    * key document, then remove the key document from the collection. */
   {
      mongoc_collection_t *const datakeys = mongoc_client_get_collection (client, "keyvault", "datakeys");
      mongoc_cursor_t *cursor =
         mongoc_collection_find_with_opts (datakeys, tmp_bson ("{}"), NULL /* opts */, NULL /* read prefs */);
      const bson_t *bson;

      ASSERT (mongoc_cursor_next (cursor, &bson));
      bson_copy_to (bson, &datakey);
      mongoc_cursor_destroy (cursor);

      (void) mongoc_collection_drop (datakeys, &error);
      mongoc_collection_destroy (datakeys);
   }

   /* Replace the _id field in the copied key document with a UUID with base64
    * value AAAAAAAAAAAAAAAAAAAAAA== (16 bytes all equal to 0x00) and insert the
    * modified key document into keyvault.datakeys with majority write concern.
    */
   {
      mongoc_collection_t *const datakeys = mongoc_client_get_collection (client, "keyvault", "datakeys");
      bson_t modified_datakey = BSON_INITIALIZER;
      uint8_t bytes[16] = {0};
      mongoc_write_concern_t *const wc = mongoc_write_concern_new ();
      bson_t opts = BSON_INITIALIZER;

      bson_copy_to_excluding_noinit (&datakey, &modified_datakey, "_id", NULL);
      BSON_ASSERT (BSON_APPEND_BINARY (&modified_datakey, "_id", BSON_SUBTYPE_UUID, bytes, sizeof (bytes)));

      mongoc_write_concern_set_w (wc, MONGOC_WRITE_CONCERN_W_MAJORITY);
      mongoc_write_concern_append (wc, &opts);

      ASSERT_OR_PRINT (mongoc_collection_insert_one (datakeys, &modified_datakey, &opts, NULL, &error), error);

      mongoc_collection_destroy (datakeys);
      bson_destroy (&modified_datakey);
      mongoc_write_concern_destroy (wc);
      bson_destroy (&opts);
   }

   /* Using client_encryption, encrypt the string "test" with the modified data
    * key using the AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic algorithm and
    * assert the resulting value is equal to the following (given as base64): */
   {
      const char expected[] = "AQAAAAAAAAAAAAAAAAAAAAACz0ZOLuuhEYi807ZXTdhbqhLaS2/"
                              "t9wLifJnnNYwiw79d75QYIZ6M/aYC1h9nCzCjZ7pGUpAuNnkUhnIXM3PjrA==";
      mongoc_client_encryption_encrypt_opts_t *const encrypt_opts = mongoc_client_encryption_encrypt_opts_new ();
      bson_value_t keyid = {0};
      bson_value_t to_encrypt = {0};
      bson_value_t ciphertext = {0};

      keyid.value_type = BSON_TYPE_BINARY;
      keyid.value.v_binary.subtype = BSON_SUBTYPE_UUID;
      keyid.value.v_binary.data = bson_malloc0 (16);
      keyid.value.v_binary.data_len = 16u;

      to_encrypt.value_type = BSON_TYPE_UTF8;
      to_encrypt.value.v_utf8.str = bson_strdup ("test");
      to_encrypt.value.v_utf8.len = 4u;

      mongoc_client_encryption_encrypt_opts_set_keyid (encrypt_opts, &keyid);
      mongoc_client_encryption_encrypt_opts_set_algorithm (encrypt_opts,
                                                           MONGOC_AEAD_AES_256_CBC_HMAC_SHA_512_DETERMINISTIC);
      ASSERT_OR_PRINT (
         mongoc_client_encryption_encrypt (client_encryption, &to_encrypt, encrypt_opts, &ciphertext, &error), error);

      {
         char actual[256];

         /* Need room for null terminator. */
         ASSERT (mcommon_b64_ntop (
                    ciphertext.value.v_binary.data, ciphertext.value.v_binary.data_len, actual, sizeof (actual)) < 255);

         ASSERT_CMPSTR (expected, actual);
      }

      bson_value_destroy (&keyid);
      bson_value_destroy (&ciphertext);
      bson_value_destroy (&to_encrypt);
      mongoc_client_encryption_encrypt_opts_destroy (encrypt_opts);
   }

   mongoc_client_destroy (client);
   mongoc_client_encryption_destroy (client_encryption);
   bson_destroy (&datakey);
}

/* Prose Test 2: Data Key and Double Encryption */
static void
test_datakey_and_double_encryption (void *unused)
{
   mongoc_client_t *client;
   mongoc_client_t *client_encrypted;
   mongoc_client_encryption_t *client_encryption;
   mongoc_apm_callbacks_t *callbacks;
   mongoc_collection_t *coll;
   bson_error_t error;
   bson_t *kms_providers;
   bson_t *tls_opts;
   mongoc_auto_encryption_opts_t *auto_encryption_opts;
   mongoc_client_encryption_opts_t *client_encryption_opts;
   bson_t *schema_map;
   bool ret;
   _datakey_and_double_encryption_ctx_t test_ctx = {0};

   BSON_UNUSED (unused);

   /* Test setup */
   /* Create a MongoClient without encryption enabled (referred to as client).
    * Enable command monitoring to listen for command_started events. */
   client = test_framework_new_default_client ();
   callbacks = mongoc_apm_callbacks_new ();
   mongoc_apm_set_command_started_cb (callbacks, _datakey_and_double_encryption_command_started);
   mongoc_client_set_apm_callbacks (client, callbacks, &test_ctx);

   /* Using client, drop the collections keyvault.datakeys and db.coll. */
   coll = mongoc_client_get_collection (client, "keyvault", "datakeys");
   (void) mongoc_collection_drop (coll, NULL);
   mongoc_collection_destroy (coll);
   coll = mongoc_client_get_collection (client, "db", "coll");
   (void) mongoc_collection_drop (coll, NULL);
   mongoc_collection_destroy (coll);

   /* Create a MongoClient configured with auto encryption (referred to as
    * client_encrypted) */
   auto_encryption_opts = mongoc_auto_encryption_opts_new ();
   kms_providers = _make_kms_providers (true /* aws */, true /* local */);
   tls_opts = _make_tls_opts ();
   _check_bypass (auto_encryption_opts);
   mongoc_auto_encryption_opts_set_kms_providers (auto_encryption_opts, kms_providers);
   mongoc_auto_encryption_opts_set_tls_opts (auto_encryption_opts, tls_opts);
   mongoc_auto_encryption_opts_set_keyvault_namespace (auto_encryption_opts, "keyvault", "datakeys");
   schema_map = get_bson_from_json_file ("./src/libmongoc/tests/client_side_encryption_prose/"
                                         "datakey-and-double-encryption-schemamap.json");
   mongoc_auto_encryption_opts_set_schema_map (auto_encryption_opts, schema_map);

   client_encrypted = test_framework_new_default_client ();
   ret = mongoc_client_enable_auto_encryption (client_encrypted, auto_encryption_opts, &error);
   ASSERT_OR_PRINT (ret, error);

   /* Create a ClientEncryption object (referred to as client_encryption) */
   client_encryption_opts = mongoc_client_encryption_opts_new ();
   mongoc_client_encryption_opts_set_kms_providers (client_encryption_opts, kms_providers);
   mongoc_client_encryption_opts_set_tls_opts (client_encryption_opts, tls_opts);
   mongoc_client_encryption_opts_set_keyvault_namespace (client_encryption_opts, "keyvault", "datakeys");
   mongoc_client_encryption_opts_set_keyvault_client (client_encryption_opts, client);
   client_encryption = mongoc_client_encryption_new (client_encryption_opts, &error);
   ASSERT_OR_PRINT (client_encryption, error);

   test_datakey_and_double_encryption_creating_and_using (
      client_encryption, client, client_encrypted, "local", &test_ctx);
   test_datakey_and_double_encryption_creating_and_using (
      client_encryption, client, client_encrypted, "aws", &test_ctx);
   test_datakey_and_double_encryption_creating_and_using (
      client_encryption, client, client_encrypted, "azure", &test_ctx);
   test_datakey_and_double_encryption_creating_and_using (
      client_encryption, client, client_encrypted, "gcp", &test_ctx);
   test_datakey_and_double_encryption_creating_and_using (
      client_encryption, client, client_encrypted, "kmip", &test_ctx);

   bson_destroy (kms_providers);
   bson_destroy (tls_opts);
   bson_destroy (schema_map);
   mongoc_client_encryption_opts_destroy (client_encryption_opts);
   mongoc_auto_encryption_opts_destroy (auto_encryption_opts);
   mongoc_apm_callbacks_destroy (callbacks);
   mongoc_client_destroy (client);
   mongoc_client_destroy (client_encrypted);
   mongoc_client_encryption_destroy (client_encryption);
   bson_destroy (test_ctx.last_cmd);
}

static void
_test_key_vault (bool with_external_key_vault)
{
   mongoc_client_t *client;
   mongoc_client_t *client_encrypted;
   mongoc_client_t *client_external;
   mongoc_client_encryption_t *client_encryption;
   mongoc_uri_t *external_uri;
   mongoc_collection_t *coll;
   bson_t *datakey;
   bson_t *kms_providers;
   bson_error_t error;
   mongoc_write_concern_t *wc;
   bson_t *schema;
   bson_t *schema_map;
   mongoc_auto_encryption_opts_t *auto_encryption_opts;
   mongoc_client_encryption_opts_t *client_encryption_opts;
   mongoc_client_encryption_encrypt_opts_t *encrypt_opts;
   bool res;
   const bson_value_t *keyid;
   bson_value_t value;
   bson_value_t ciphertext;
   bson_iter_t iter;

   external_uri = test_framework_get_uri ();
   mongoc_uri_set_username (external_uri, "fake-user");
   mongoc_uri_set_password (external_uri, "fake-pwd");
   client_external = test_framework_client_new_from_uri (external_uri, NULL);
   test_framework_set_ssl_opts (client_external);

   /* Using client, drop the collections keyvault.datakeys and db.coll. */
   client = test_framework_new_default_client ();
   coll = mongoc_client_get_collection (client, "db", "coll");
   (void) mongoc_collection_drop (coll, NULL);
   mongoc_collection_destroy (coll);
   coll = mongoc_client_get_collection (client, "keyvault", "datakeys");
   (void) mongoc_collection_drop (coll, NULL);

   /* Insert the document external-key.json into ``keyvault.datakeys``. */
   wc = mongoc_write_concern_new ();
   mongoc_write_concern_set_wmajority (wc, 1000);
   mongoc_collection_set_write_concern (coll, wc);
   datakey = get_bson_from_json_file ("./src/libmongoc/tests/"
                                      "client_side_encryption_prose/external/"
                                      "external-key.json");
   ASSERT_OR_PRINT (mongoc_collection_insert_one (coll, datakey, NULL, NULL, &error), error);
   mongoc_collection_destroy (coll);

   /* Create a MongoClient configured with auto encryption. */
   client_encrypted = test_framework_new_default_client ();
   mongoc_client_set_error_api (client_encrypted, MONGOC_ERROR_API_VERSION_2);
   auto_encryption_opts = mongoc_auto_encryption_opts_new ();
   _check_bypass (auto_encryption_opts);
   schema = get_bson_from_json_file ("./src/libmongoc/tests/"
                                     "client_side_encryption_prose/external/"
                                     "external-schema.json");
   schema_map = BCON_NEW ("db.coll", BCON_DOCUMENT (schema));
   kms_providers = _make_kms_providers (false /* aws */, true /* local */);
   mongoc_auto_encryption_opts_set_kms_providers (auto_encryption_opts, kms_providers);
   mongoc_auto_encryption_opts_set_keyvault_namespace (auto_encryption_opts, "keyvault", "datakeys");
   mongoc_auto_encryption_opts_set_schema_map (auto_encryption_opts, schema_map);
   if (with_external_key_vault) {
      mongoc_auto_encryption_opts_set_keyvault_client (auto_encryption_opts, client_external);
   }
   ASSERT_OR_PRINT (mongoc_client_enable_auto_encryption (client_encrypted, auto_encryption_opts, &error), error);

   /* Create a ClientEncryption object. */
   client_encryption_opts = mongoc_client_encryption_opts_new ();
   mongoc_client_encryption_opts_set_kms_providers (client_encryption_opts, kms_providers);
   mongoc_client_encryption_opts_set_keyvault_namespace (client_encryption_opts, "keyvault", "datakeys");
   if (with_external_key_vault) {
      mongoc_client_encryption_opts_set_keyvault_client (client_encryption_opts, client_external);
   } else {
      mongoc_client_encryption_opts_set_keyvault_client (client_encryption_opts, client);
   }
   client_encryption = mongoc_client_encryption_new (client_encryption_opts, &error);
   ASSERT_OR_PRINT (client_encryption, error);

   /* Use client_encrypted to insert the document {"encrypted": "test"} into
    * db.coll. */
   coll = mongoc_client_get_collection (client_encrypted, "db", "coll");
   res = mongoc_collection_insert_one (coll, tmp_bson ("{'encrypted': 'test'}"), NULL, NULL, &error);
   if (with_external_key_vault) {
      BSON_ASSERT (!res);
      ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_CLIENT, MONGOC_ERROR_CLIENT_AUTHENTICATE, "Authentication failed");
   } else {
      ASSERT_OR_PRINT (res, error);
   }

   /* Use client_encryption to explicitly encrypt the string "test" with key ID
    * ``LOCALAAAAAAAAAAAAAAAAA==`` and deterministic algorithm. */
   encrypt_opts = mongoc_client_encryption_encrypt_opts_new ();
   mongoc_client_encryption_encrypt_opts_set_algorithm (encrypt_opts,
                                                        MONGOC_AEAD_AES_256_CBC_HMAC_SHA_512_DETERMINISTIC);
   BSON_ASSERT (bson_iter_init_find (&iter, datakey, "_id"));
   keyid = bson_iter_value (&iter);
   mongoc_client_encryption_encrypt_opts_set_keyid (encrypt_opts, keyid);
   value.value_type = BSON_TYPE_UTF8;
   value.value.v_utf8.str = "test";
   value.value.v_utf8.len = 4;
   res = mongoc_client_encryption_encrypt (client_encryption, &value, encrypt_opts, &ciphertext, &error);
   if (with_external_key_vault) {
      BSON_ASSERT (!res);
      ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_CLIENT, MONGOC_ERROR_CLIENT_AUTHENTICATE, "Authentication failed");
   } else {
      ASSERT_OR_PRINT (res, error);
   }

   bson_destroy (schema);
   bson_destroy (schema_map);
   bson_destroy (datakey);
   bson_value_destroy (&ciphertext);
   mongoc_write_concern_destroy (wc);
   bson_destroy (kms_providers);
   mongoc_collection_destroy (coll);
   mongoc_client_destroy (client);
   mongoc_client_destroy (client_encrypted);
   mongoc_client_destroy (client_external);
   mongoc_uri_destroy (external_uri);
   mongoc_auto_encryption_opts_destroy (auto_encryption_opts);
   mongoc_client_encryption_opts_destroy (client_encryption_opts);
   mongoc_client_encryption_destroy (client_encryption);
   mongoc_client_encryption_encrypt_opts_destroy (encrypt_opts);
}

/* Prose Test 3: External Key Vault Test */
static void
test_external_key_vault (void *unused)
{
   BSON_UNUSED (unused);

   _test_key_vault (false /* external */);
   _test_key_vault (true /* external */);
}

/* Prose Test 5: Views Are Prohibited */
static void
test_views_are_prohibited (void *unused)
{
   mongoc_client_t *client;
   mongoc_client_t *client_encrypted;
   mongoc_collection_t *coll;
   bool res;
   bson_error_t error;

   mongoc_auto_encryption_opts_t *auto_encryption_opts;
   bson_t *kms_providers;

   BSON_UNUSED (unused);

   client = test_framework_new_default_client ();

   /* Using client, drop and create a view named db.view with an empty pipeline.
    * E.g. using the command { "create": "view", "viewOn": "coll" }. */
   coll = mongoc_client_get_collection (client, "db", "view");
   (void) mongoc_collection_drop (coll, NULL);
   res = mongoc_client_command_simple (
      client, "db", tmp_bson ("{'create': 'view', 'viewOn': 'coll'}"), NULL, NULL, &error);
   ASSERT_OR_PRINT (res, error);

   client_encrypted = test_framework_new_default_client ();
   auto_encryption_opts = mongoc_auto_encryption_opts_new ();
   _check_bypass (auto_encryption_opts);
   kms_providers = _make_kms_providers (false /* aws */, true /* local */);
   mongoc_auto_encryption_opts_set_kms_providers (auto_encryption_opts, kms_providers);
   mongoc_auto_encryption_opts_set_keyvault_namespace (auto_encryption_opts, "keyvault", "datakeys");
   ASSERT_OR_PRINT (mongoc_client_enable_auto_encryption (client_encrypted, auto_encryption_opts, &error), error);

   mongoc_collection_destroy (coll);
   coll = mongoc_client_get_collection (client_encrypted, "db", "view");
   res = mongoc_collection_insert_one (coll, tmp_bson ("{'x': 1}"), NULL, NULL, &error);
   BSON_ASSERT (!res);
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_CLIENT_SIDE_ENCRYPTION, 1, "cannot auto encrypt a view");

   bson_destroy (kms_providers);
   mongoc_collection_destroy (coll);
   mongoc_auto_encryption_opts_destroy (auto_encryption_opts);
   mongoc_client_destroy (client_encrypted);
   mongoc_client_destroy (client);
}

static void
_endpoint_setup (mongoc_client_t *keyvault_client,
                 mongoc_client_encryption_t **client_encryption,
                 mongoc_client_encryption_t **client_encryption_invalid)
{
   bson_t *tls_opts;
   bson_t *kms_providers;
   bson_t *kms_providers_invalid;
   mongoc_client_encryption_opts_t *client_encryption_opts;
   mongoc_client_encryption_opts_t *client_encryption_opts_invalid;
   bson_error_t error;

   ASSERT (keyvault_client);

   char *mongoc_test_aws_access_key_id = test_framework_getenv_required ("MONGOC_TEST_AWS_ACCESS_KEY_ID");
   char *mongoc_test_aws_secret_access_key = test_framework_getenv_required ("MONGOC_TEST_AWS_SECRET_ACCESS_KEY");
   char *mongoc_test_azure_tenant_id = test_framework_getenv_required ("MONGOC_TEST_AZURE_TENANT_ID");
   char *mongoc_test_azure_client_id = test_framework_getenv_required ("MONGOC_TEST_AZURE_CLIENT_ID");
   char *mongoc_test_azure_client_secret = test_framework_getenv_required ("MONGOC_TEST_AZURE_CLIENT_SECRET");
   char *mongoc_test_gcp_email = test_framework_getenv_required ("MONGOC_TEST_GCP_EMAIL");
   char *mongoc_test_gcp_privatekey = test_framework_getenv_required ("MONGOC_TEST_GCP_PRIVATEKEY");
   char *ca_file = test_framework_getenv_required ("MONGOC_TEST_CSFLE_TLS_CA_FILE");
   char *certificate_key_file = test_framework_getenv_required ("MONGOC_TEST_CSFLE_TLS_CERTIFICATE_KEY_FILE");


   kms_providers = tmp_bson ("{'aws': {'accessKeyId': '%s', 'secretAccessKey': '%s'}}",
                             mongoc_test_aws_access_key_id,
                             mongoc_test_aws_secret_access_key);
   bson_concat (kms_providers,
                tmp_bson ("{'azure': {'tenantId': '%s', 'clientId': '%s', 'clientSecret': '%s', "
                          "'identityPlatformEndpoint': 'login.microsoftonline.com:443'}}",
                          mongoc_test_azure_tenant_id,
                          mongoc_test_azure_client_id,
                          mongoc_test_azure_client_secret));
   bson_concat (kms_providers,
                tmp_bson ("{'gcp': { 'email': '%s', 'privateKey': '%s', "
                          "'endpoint': 'oauth2.googleapis.com:443'}}",
                          mongoc_test_gcp_email,
                          mongoc_test_gcp_privatekey));
   bson_concat (kms_providers, tmp_bson ("{'kmip': { 'endpoint': 'localhost:5698' }}"));
   tls_opts =
      tmp_bson ("{'kmip': {  'tlsCAFile': '%s', 'tlsCertificateKeyFile': '%s' }}", ca_file, certificate_key_file);

   kms_providers_invalid = tmp_bson ("{'aws': {'accessKeyId': '%s', 'secretAccessKey': '%s'}}",
                                     mongoc_test_aws_access_key_id,
                                     mongoc_test_aws_secret_access_key);
   bson_concat (kms_providers_invalid,
                tmp_bson ("{'azure': {'tenantId': '%s', 'clientId': '%s', 'clientSecret': '%s', "
                          "'identityPlatformEndpoint': 'doesnotexist.invalid:443'}}",
                          mongoc_test_azure_tenant_id,
                          mongoc_test_azure_client_id,
                          mongoc_test_azure_client_secret));
   bson_concat (kms_providers_invalid,
                tmp_bson ("{'gcp': { 'email': '%s', 'privateKey': '%s', "
                          "'endpoint': 'doesnotexist.invalid'}}",
                          mongoc_test_gcp_email,
                          mongoc_test_gcp_privatekey));
   bson_concat (kms_providers_invalid, tmp_bson ("{'kmip': { 'endpoint': 'doesnotexist.local:5698' }}"));

   client_encryption_opts = mongoc_client_encryption_opts_new ();
   mongoc_client_encryption_opts_set_kms_providers (client_encryption_opts, kms_providers);
   mongoc_client_encryption_opts_set_tls_opts (client_encryption_opts, tls_opts);
   mongoc_client_encryption_opts_set_keyvault_namespace (client_encryption_opts, "keyvault", "datakeys");
   mongoc_client_encryption_opts_set_keyvault_client (client_encryption_opts, keyvault_client);
   *client_encryption = mongoc_client_encryption_new (client_encryption_opts, &error);
   ASSERT_OR_PRINT (client_encryption, error);

   client_encryption_opts_invalid = mongoc_client_encryption_opts_new ();
   mongoc_client_encryption_opts_set_kms_providers (client_encryption_opts_invalid, kms_providers_invalid);
   mongoc_client_encryption_opts_set_tls_opts (client_encryption_opts_invalid, tls_opts);
   mongoc_client_encryption_opts_set_keyvault_namespace (client_encryption_opts_invalid, "keyvault", "datakeys");
   mongoc_client_encryption_opts_set_keyvault_client (client_encryption_opts_invalid, keyvault_client);
   *client_encryption_invalid = mongoc_client_encryption_new (client_encryption_opts_invalid, &error);
   ASSERT_OR_PRINT (client_encryption_invalid, error);

   mongoc_client_encryption_opts_destroy (client_encryption_opts);
   mongoc_client_encryption_opts_destroy (client_encryption_opts_invalid);
   bson_free (mongoc_test_aws_access_key_id);
   bson_free (mongoc_test_aws_secret_access_key);
   bson_free (mongoc_test_azure_tenant_id);
   bson_free (mongoc_test_azure_client_id);
   bson_free (mongoc_test_azure_client_secret);
   bson_free (mongoc_test_gcp_email);
   bson_free (mongoc_test_gcp_privatekey);
   bson_free (ca_file);
   bson_free (certificate_key_file);
}

/* Use the returned UUID of the key to explicitly encrypt and decrypt the
 * string "test" to validate it works. */
#define TEST_ENCRYPT_DECRYPT(keyid, client_encryption, res, error)                                              \
   do {                                                                                                         \
      bson_value_t ciphertext;                                                                                  \
      bson_value_t plaintext;                                                                                   \
      bson_value_t test;                                                                                        \
      mongoc_client_encryption_encrypt_opts_t *encrypt_opts;                                                    \
                                                                                                                \
      test.value_type = BSON_TYPE_UTF8;                                                                         \
      test.value.v_utf8.str = "test";                                                                           \
      test.value.v_utf8.len = 4;                                                                                \
                                                                                                                \
      encrypt_opts = mongoc_client_encryption_encrypt_opts_new ();                                              \
      mongoc_client_encryption_encrypt_opts_set_algorithm (encrypt_opts,                                        \
                                                           MONGOC_AEAD_AES_256_CBC_HMAC_SHA_512_DETERMINISTIC); \
      mongoc_client_encryption_encrypt_opts_set_keyid (encrypt_opts, keyid);                                    \
      res = mongoc_client_encryption_encrypt (client_encryption, &test, encrypt_opts, &ciphertext, &error);     \
      ASSERT_OR_PRINT (res, error);                                                                             \
      res = mongoc_client_encryption_decrypt (client_encryption, &ciphertext, &plaintext, &error);              \
      ASSERT_OR_PRINT (res, error);                                                                             \
      if (plaintext.value_type != BSON_TYPE_UTF8) {                                                             \
         test_error ("expected decrypted result to be value type UTF-8, got %s",                                \
                     _mongoc_bson_type_to_str (plaintext.value_type));                                          \
      }                                                                                                         \
      ASSERT_CMPSTR (plaintext.value.v_utf8.str, test.value.v_utf8.str);                                        \
      bson_value_destroy (&ciphertext);                                                                         \
      bson_value_destroy (&plaintext);                                                                          \
      mongoc_client_encryption_encrypt_opts_destroy (encrypt_opts);                                             \
   } while (0)

/* Prose Test 7: Custom Endpoint Test */
static void
test_custom_endpoint (void *unused)
{
   mongoc_client_t *keyvault_client;
   mongoc_client_encryption_t *client_encryption = NULL;
   mongoc_client_encryption_t *client_encryption_invalid = NULL;
   mongoc_client_encryption_datakey_opts_t *datakey_opts;
   bson_error_t error;
   bool res;
   bson_t *masterkey;
   bson_value_t keyid;

   BSON_UNUSED (unused);

   keyvault_client = test_framework_new_default_client ();

   datakey_opts = mongoc_client_encryption_datakey_opts_new ();

   /* Case 1: No endpoint, expect to succeed. */
   _endpoint_setup (keyvault_client, &client_encryption, &client_encryption_invalid);
   masterkey = BCON_NEW ("region",
                         "us-east-1",
                         "key",
                         "arn:aws:kms:us-east-1:579766882180:key/"
                         "89fcc2c4-08b0-4bd9-9f25-e30687b580d0");
   mongoc_client_encryption_datakey_opts_set_masterkey (datakey_opts, masterkey);
   res = mongoc_client_encryption_create_datakey (client_encryption, "aws", datakey_opts, &keyid, &error);
   ASSERT_OR_PRINT (res, error);

   TEST_ENCRYPT_DECRYPT (&keyid, client_encryption, res, error);
   bson_value_destroy (&keyid);
   bson_destroy (masterkey);
   mongoc_client_encryption_destroy (client_encryption);
   mongoc_client_encryption_destroy (client_encryption_invalid);

   /* Case 2: Custom endpoint, with the same as the default. Expect to succeed
    */
   _endpoint_setup (keyvault_client, &client_encryption, &client_encryption_invalid);
   masterkey = BCON_NEW ("region",
                         "us-east-1",
                         "key",
                         "arn:aws:kms:us-east-1:579766882180:key/"
                         "89fcc2c4-08b0-4bd9-9f25-e30687b580d0",
                         "endpoint",
                         "kms.us-east-1.amazonaws.com");
   mongoc_client_encryption_datakey_opts_set_masterkey (datakey_opts, masterkey);
   res = mongoc_client_encryption_create_datakey (client_encryption, "aws", datakey_opts, &keyid, &error);
   ASSERT_OR_PRINT (res, error);

   TEST_ENCRYPT_DECRYPT (&keyid, client_encryption, res, error);
   bson_value_destroy (&keyid);
   bson_destroy (masterkey);
   mongoc_client_encryption_destroy (client_encryption);
   mongoc_client_encryption_destroy (client_encryption_invalid);

   /* Case 3: Custom endpoint, with the same as the default but port included.
    * Expect to succeed */
   _endpoint_setup (keyvault_client, &client_encryption, &client_encryption_invalid);
   masterkey = BCON_NEW ("region",
                         "us-east-1",
                         "key",
                         "arn:aws:kms:us-east-1:579766882180:key/"
                         "89fcc2c4-08b0-4bd9-9f25-e30687b580d0",
                         "endpoint",
                         "kms.us-east-1.amazonaws.com:443");
   mongoc_client_encryption_datakey_opts_set_masterkey (datakey_opts, masterkey);
   res = mongoc_client_encryption_create_datakey (client_encryption, "aws", datakey_opts, &keyid, &error);
   ASSERT_OR_PRINT (res, error);

   TEST_ENCRYPT_DECRYPT (&keyid, client_encryption, res, error);
   bson_value_destroy (&keyid);
   bson_destroy (masterkey);
   mongoc_client_encryption_destroy (client_encryption);
   mongoc_client_encryption_destroy (client_encryption_invalid);

   /* Case 4: Custom endpoint, with the same as the default but wrong port
    * included.
    * Expect to fail with socket error */
   _endpoint_setup (keyvault_client, &client_encryption, &client_encryption_invalid);
   masterkey = BCON_NEW ("region",
                         "us-east-1",
                         "key",
                         "arn:aws:kms:us-east-1:579766882180:key/"
                         "89fcc2c4-08b0-4bd9-9f25-e30687b580d0",
                         "endpoint",
                         "kms.us-east-1.amazonaws.com:12345");
   mongoc_client_encryption_datakey_opts_set_masterkey (datakey_opts, masterkey);
   res = mongoc_client_encryption_create_datakey (client_encryption, "aws", datakey_opts, &keyid, &error);
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_CONNECT, "Failed to connect");
   BSON_ASSERT (!res);
   bson_value_destroy (&keyid);
   bson_destroy (masterkey);
   mongoc_client_encryption_destroy (client_encryption);
   mongoc_client_encryption_destroy (client_encryption_invalid);

   /* Case 5: Custom endpoint, but wrong region. */
   _endpoint_setup (keyvault_client, &client_encryption, &client_encryption_invalid);
   masterkey = BCON_NEW ("region",
                         "us-east-1",
                         "key",
                         "arn:aws:kms:us-east-1:579766882180:key/"
                         "89fcc2c4-08b0-4bd9-9f25-e30687b580d0",
                         "endpoint",
                         "kms.us-east-2.amazonaws.com");
   mongoc_client_encryption_datakey_opts_set_masterkey (datakey_opts, masterkey);
   memset (&error, 0, sizeof (bson_error_t));
   res = mongoc_client_encryption_create_datakey (client_encryption, "aws", datakey_opts, &keyid, &error);
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_CLIENT_SIDE_ENCRYPTION, 1, "");
   BSON_ASSERT (!res);
   bson_value_destroy (&keyid);
   bson_destroy (masterkey);
   mongoc_client_encryption_destroy (client_encryption);
   mongoc_client_encryption_destroy (client_encryption_invalid);

   /* Case 6: Custom endpoint to doesnotexist.invalid. */
   _endpoint_setup (keyvault_client, &client_encryption, &client_encryption_invalid);
   masterkey = BCON_NEW ("region",
                         "us-east-1",
                         "key",
                         "arn:aws:kms:us-east-1:579766882180:key/"
                         "89fcc2c4-08b0-4bd9-9f25-e30687b580d0",
                         "endpoint",
                         "doesnotexist.invalid");
   mongoc_client_encryption_datakey_opts_set_masterkey (datakey_opts, masterkey);
   memset (&error, 0, sizeof (bson_error_t));
   res = mongoc_client_encryption_create_datakey (client_encryption, "aws", datakey_opts, &keyid, &error);
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_NAME_RESOLUTION, "Failed to resolve");
   BSON_ASSERT (!res);
   bson_value_destroy (&keyid);
   bson_destroy (masterkey);
   mongoc_client_encryption_destroy (client_encryption);
   mongoc_client_encryption_destroy (client_encryption_invalid);

   /* Case 7: Azure successful case */
   _endpoint_setup (keyvault_client, &client_encryption, &client_encryption_invalid);
   masterkey = BCON_NEW ("keyVaultEndpoint", "key-vault-csfle.vault.azure.net", "keyName", "key-name-csfle");
   mongoc_client_encryption_datakey_opts_set_masterkey (datakey_opts, masterkey);
   res = mongoc_client_encryption_create_datakey (client_encryption, "azure", datakey_opts, &keyid, &error);
   ASSERT_OR_PRINT (res, error);

   TEST_ENCRYPT_DECRYPT (&keyid, client_encryption, res, error);
   bson_value_destroy (&keyid);
   bson_destroy (masterkey);
   mongoc_client_encryption_destroy (client_encryption);
   mongoc_client_encryption_destroy (client_encryption_invalid);

   /* Azure invalid case. */
   _endpoint_setup (keyvault_client, &client_encryption, &client_encryption_invalid);
   res = mongoc_client_encryption_create_datakey (client_encryption_invalid, "azure", datakey_opts, &keyid, &error);
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_NAME_RESOLUTION, "Failed to resolve");
   BSON_ASSERT (!res);
   mongoc_client_encryption_destroy (client_encryption);
   mongoc_client_encryption_destroy (client_encryption_invalid);

   /* Case 8: GCP successful case. */
   _endpoint_setup (keyvault_client, &client_encryption, &client_encryption_invalid);
   masterkey = BCON_NEW ("projectId",
                         "devprod-drivers",
                         "location",
                         "global",
                         "keyRing",
                         "key-ring-csfle",
                         "keyName",
                         "key-name-csfle",
                         "endpoint",
                         "cloudkms.googleapis.com:443");
   mongoc_client_encryption_datakey_opts_set_masterkey (datakey_opts, masterkey);
   res = mongoc_client_encryption_create_datakey (client_encryption, "gcp", datakey_opts, &keyid, &error);
   ASSERT_OR_PRINT (res, error);

   TEST_ENCRYPT_DECRYPT (&keyid, client_encryption, res, error);
   bson_value_destroy (&keyid);
   bson_destroy (masterkey);
   mongoc_client_encryption_destroy (client_encryption);
   mongoc_client_encryption_destroy (client_encryption_invalid);

   /* GCP invalid case. */
   _endpoint_setup (keyvault_client, &client_encryption, &client_encryption_invalid);
   res = mongoc_client_encryption_create_datakey (client_encryption_invalid, "gcp", datakey_opts, &keyid, &error);
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_NAME_RESOLUTION, "Failed to resolve");
   BSON_ASSERT (!res);
   mongoc_client_encryption_destroy (client_encryption);
   mongoc_client_encryption_destroy (client_encryption_invalid);

   /* Case 9: GCP invalid key endpoint. */
   _endpoint_setup (keyvault_client, &client_encryption, &client_encryption_invalid);
   masterkey = BCON_NEW ("projectId",
                         "devprod-drivers",
                         "location",
                         "global",
                         "keyRing",
                         "key-ring-csfle",
                         "keyName",
                         "key-name-csfle",
                         "endpoint",
                         "doesnotexist.invalid:443");
   mongoc_client_encryption_datakey_opts_set_masterkey (datakey_opts, masterkey);
   res = mongoc_client_encryption_create_datakey (client_encryption, "gcp", datakey_opts, &keyid, &error);
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_CLIENT_SIDE_ENCRYPTION, 1, "Invalid KMS response");
   BSON_ASSERT (!res);

   bson_destroy (masterkey);
   mongoc_client_encryption_destroy (client_encryption);
   mongoc_client_encryption_destroy (client_encryption_invalid);

   /* Case 10: KMIP no endpoint. */
   _endpoint_setup (keyvault_client, &client_encryption, &client_encryption_invalid);
   masterkey = BCON_NEW ("keyId", "1");
   mongoc_client_encryption_datakey_opts_set_masterkey (datakey_opts, masterkey);
   res = mongoc_client_encryption_create_datakey (client_encryption, "kmip", datakey_opts, &keyid, &error);
   ASSERT_OR_PRINT (res, error);

   TEST_ENCRYPT_DECRYPT (&keyid, client_encryption, res, error);
   bson_value_destroy (&keyid);

   /* Attempt to use client_encryption_invalid with the same masterKey. Expect
    * an error. */
   res = mongoc_client_encryption_create_datakey (client_encryption_invalid, "kmip", datakey_opts, &keyid, &error);
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_NAME_RESOLUTION, "Failed to resolve");
   BSON_ASSERT (!res);
   bson_value_destroy (&keyid);

   bson_destroy (masterkey);
   mongoc_client_encryption_destroy (client_encryption);
   mongoc_client_encryption_destroy (client_encryption_invalid);

   /* Case 11: KMIP overriding with valid endpoint. */
   _endpoint_setup (keyvault_client, &client_encryption, &client_encryption_invalid);
   masterkey = BCON_NEW ("keyId", "1", "endpoint", "localhost:5698");
   mongoc_client_encryption_datakey_opts_set_masterkey (datakey_opts, masterkey);
   res = mongoc_client_encryption_create_datakey (client_encryption, "kmip", datakey_opts, &keyid, &error);
   ASSERT_OR_PRINT (res, error);

   TEST_ENCRYPT_DECRYPT (&keyid, client_encryption, res, error);
   bson_value_destroy (&keyid);

   bson_destroy (masterkey);
   mongoc_client_encryption_destroy (client_encryption);
   mongoc_client_encryption_destroy (client_encryption_invalid);

   /* Case 12: KMIP overriding with invalid endpoint. */
   _endpoint_setup (keyvault_client, &client_encryption, &client_encryption_invalid);
   masterkey = BCON_NEW ("keyId", "1", "endpoint", "doesnotexist.local:5698");
   mongoc_client_encryption_datakey_opts_set_masterkey (datakey_opts, masterkey);
   res = mongoc_client_encryption_create_datakey (client_encryption, "kmip", datakey_opts, &keyid, &error);
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_NAME_RESOLUTION, "Failed to resolve");
   BSON_ASSERT (!res);
   bson_value_destroy (&keyid);

   bson_destroy (masterkey);
   mongoc_client_encryption_destroy (client_encryption);
   mongoc_client_encryption_destroy (client_encryption_invalid);

   mongoc_client_encryption_datakey_opts_destroy (datakey_opts);
   mongoc_client_destroy (keyvault_client);
}

typedef struct {
   const char *kms;
   const char *type;
   const char *algo;
   const char *method;
   const char *identifier;
   bool allowed;
   bson_value_t value; /* a copy */
} corpus_field_t;

static corpus_field_t *
_corpus_field_new (bson_iter_t *top_iter)
{
   bson_iter_t iter;
   corpus_field_t *field;

   field = bson_malloc0 (sizeof (corpus_field_t));
   memset (field, 0, sizeof (*field));
   BSON_ASSERT (BSON_ITER_HOLDS_DOCUMENT (top_iter));
   bson_iter_recurse (top_iter, &iter);
   while (bson_iter_next (&iter)) {
      if (0 == strcmp ("kms", bson_iter_key (&iter))) {
         field->kms = bson_iter_utf8 (&iter, NULL);
      } else if (0 == strcmp ("type", bson_iter_key (&iter))) {
         field->type = bson_iter_utf8 (&iter, NULL);
      } else if (0 == strcmp ("algo", bson_iter_key (&iter))) {
         field->algo = bson_iter_utf8 (&iter, NULL);
      } else if (0 == strcmp ("method", bson_iter_key (&iter))) {
         field->method = bson_iter_utf8 (&iter, NULL);
      } else if (0 == strcmp ("identifier", bson_iter_key (&iter))) {
         field->identifier = bson_iter_utf8 (&iter, NULL);
      } else if (0 == strcmp ("allowed", bson_iter_key (&iter))) {
         field->allowed = bson_iter_bool (&iter);
      } else if (0 == strcmp ("value", bson_iter_key (&iter))) {
         bson_value_copy (bson_iter_value (&iter), &field->value);
      } else {
         fprintf (stderr, "unexpected field: %s\n", bson_iter_key (&iter));
         BSON_ASSERT (false);
      }
   }
   return field;
}

static void
_corpus_field_destroy (corpus_field_t *field)
{
   if (!field) {
      return;
   }
   bson_value_destroy (&field->value);
   bson_free (field);
}

#define LOCAL_UUID "\x2c\xe0\x80\x2c\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
#define AWS_UUID "\x01\x64\x80\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
#define AZURE_UUID "\x01\x95\x11\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
#define GCP_UUID "\x18\x23\xc0\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
#define KMIP_UUID "\x28\xc2\x0f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"

static void
_corpus_copy_field (mongoc_client_encryption_t *client_encryption, bson_iter_t *iter, bson_t *corpus_copied)
{
   corpus_field_t *field;
   const char *key = bson_iter_key (iter);
   mongoc_client_encryption_encrypt_opts_t *encrypt_opts;
   bson_value_t ciphertext;
   bool res;
   bson_error_t error;

   if (0 == strcmp ("_id", key) || 0 == strcmp ("altname_aws", key) || 0 == strcmp ("altname_local", key) ||
       0 == strcmp ("altname_azure", key) || 0 == strcmp ("altname_gcp", key) || 0 == strcmp ("altname_kmip", key)) {
      bson_append_value (corpus_copied, key, -1, bson_iter_value (iter));
      return;
   }
   field = _corpus_field_new (iter);

   if (0 == strcmp ("auto", field->method)) {
      bson_append_value (corpus_copied, key, -1, bson_iter_value (iter));
      _corpus_field_destroy (field);
      return;
   }

   /* Otherwise, use explicit encryption. */
   encrypt_opts = mongoc_client_encryption_encrypt_opts_new ();
   if (0 == strcmp ("rand", field->algo)) {
      mongoc_client_encryption_encrypt_opts_set_algorithm (encrypt_opts, MONGOC_AEAD_AES_256_CBC_HMAC_SHA_512_RANDOM);
   } else if (0 == strcmp ("det", field->algo)) {
      mongoc_client_encryption_encrypt_opts_set_algorithm (encrypt_opts,
                                                           MONGOC_AEAD_AES_256_CBC_HMAC_SHA_512_DETERMINISTIC);
   }

   if (0 == strcmp ("id", field->identifier)) {
      bson_value_t uuid;
      uuid.value_type = BSON_TYPE_BINARY;
      uuid.value.v_binary.subtype = BSON_SUBTYPE_UUID;
      uuid.value.v_binary.data_len = 16;
      if (0 == strcmp ("local", field->kms)) {
         uuid.value.v_binary.data = (uint8_t *) LOCAL_UUID;
      } else if (0 == strcmp ("aws", field->kms)) {
         uuid.value.v_binary.data = (uint8_t *) AWS_UUID;
      } else if (0 == strcmp ("azure", field->kms)) {
         uuid.value.v_binary.data = (uint8_t *) AZURE_UUID;
      } else if (0 == strcmp ("gcp", field->kms)) {
         uuid.value.v_binary.data = (uint8_t *) GCP_UUID;
      } else if (0 == strcmp ("kmip", field->kms)) {
         uuid.value.v_binary.data = (uint8_t *) KMIP_UUID;
      }
      mongoc_client_encryption_encrypt_opts_set_keyid (encrypt_opts, &uuid);
   } else if (0 == strcmp ("altname", field->identifier)) {
      mongoc_client_encryption_encrypt_opts_set_keyaltname (encrypt_opts, field->kms);
   }

   res = mongoc_client_encryption_encrypt (client_encryption, &field->value, encrypt_opts, &ciphertext, &error);

   if (field->allowed) {
      bson_t new_field;
      ASSERT_OR_PRINT (res, error);
      bson_append_document_begin (corpus_copied, key, -1, &new_field);
      BSON_APPEND_UTF8 (&new_field, "kms", field->kms);
      BSON_APPEND_UTF8 (&new_field, "type", field->type);
      BSON_APPEND_UTF8 (&new_field, "algo", field->algo);
      BSON_APPEND_UTF8 (&new_field, "method", field->method);
      BSON_APPEND_UTF8 (&new_field, "identifier", field->identifier);
      BSON_APPEND_BOOL (&new_field, "allowed", field->allowed);
      BSON_APPEND_VALUE (&new_field, "value", &ciphertext);
      bson_append_document_end (corpus_copied, &new_field);
   } else {
      BSON_ASSERT (!res);
      bson_append_value (corpus_copied, key, -1, bson_iter_value (iter));
   }

   bson_value_destroy (&ciphertext);
   mongoc_client_encryption_encrypt_opts_destroy (encrypt_opts);
   _corpus_field_destroy (field);
}

static void
_corpus_check_encrypted (mongoc_client_encryption_t *client_encryption,
                         bson_iter_t *expected_iter,
                         bson_iter_t *actual_iter)
{
   corpus_field_t *expected;
   corpus_field_t *actual;
   const char *key;
   bson_error_t error;
   match_ctx_t match_ctx;

   memset (&match_ctx, 0, sizeof (match_ctx));
   key = bson_iter_key (expected_iter);
   if (0 == strcmp ("_id", key) || 0 == strcmp ("altname_aws", key) || 0 == strcmp ("altname_local", key) ||
       0 == strcmp ("altname_azure", key) || 0 == strcmp ("altname_gcp", key) || 0 == strcmp ("altname_kmip", key)) {
      return;
   }

   expected = _corpus_field_new (expected_iter);
   actual = _corpus_field_new (actual_iter);

   /* If the algo is det, that the value equals the value of the corresponding
    * field
    * in corpus_encrypted_actual.
    */
   if (0 == strcmp (expected->algo, "det")) {
      BSON_ASSERT (match_bson_value (&expected->value, &actual->value, &match_ctx));
   }

   /* If the algo is rand and allowed is true, that the value does not equal the
    * value of the corresponding field in corpus_encrypted_actual. */
   if (0 == strcmp (expected->algo, "rand") && expected->allowed) {
      BSON_ASSERT (!match_bson_value (&expected->value, &actual->value, &match_ctx));
   }

   /* If allowed is true, decrypt the value with client_encryption. Decrypt the
    * value of the corresponding field of corpus_encrypted and validate that
    * they are both equal */
   if (expected->allowed) {
      bson_value_t expected_decrypted;
      bson_value_t actual_decrypted;
      bool res;

      res = mongoc_client_encryption_decrypt (client_encryption, &expected->value, &expected_decrypted, &error);
      ASSERT_OR_PRINT (res, error);

      res = mongoc_client_encryption_decrypt (client_encryption, &actual->value, &actual_decrypted, &error);
      ASSERT_OR_PRINT (res, error);

      BSON_ASSERT (match_bson_value (&expected_decrypted, &actual_decrypted, &match_ctx));
      bson_value_destroy (&expected_decrypted);
      bson_value_destroy (&actual_decrypted);
   }

   /* If allowed is false, validate the value exactly equals the value of the
    * corresponding field of corpus (neither was encrypted). */
   if (!expected->allowed) {
      BSON_ASSERT (match_bson_value (&expected->value, &actual->value, &match_ctx));
   }

   _corpus_field_destroy (expected);
   _corpus_field_destroy (actual);
}

static void
_insert_from_file (mongoc_collection_t *coll, char *path)
{
   bson_t *datakey;
   bool res;
   bson_error_t error;

   datakey = get_bson_from_json_file (path);
   res = mongoc_collection_insert_one (coll, datakey, NULL, NULL, &error);
   ASSERT_OR_PRINT (res, error);
   bson_destroy (datakey);
}

static void
_test_corpus (bool local_schema)
{
   mongoc_client_t *client;
   mongoc_collection_t *coll;
   bson_t *schema;
   bson_t *create_cmd;
   bool res;
   bson_error_t error;
   mongoc_write_concern_t *wc;
   mongoc_client_t *client_encrypted;
   mongoc_auto_encryption_opts_t *auto_encryption_opts;
   bson_t *kms_providers;
   mongoc_client_encryption_t *client_encryption;
   mongoc_client_encryption_opts_t *client_encryption_opts;
   bson_t *corpus;
   bson_t corpus_copied;
   const bson_t *corpus_decrypted;
   bson_t *corpus_encrypted_expected;
   const bson_t *corpus_encrypted_actual;
   bson_iter_t iter;
   mongoc_cursor_t *cursor;
   bson_t *schema_map;
   bson_t *tls_opts;

   /* Create a MongoClient without encryption enabled */
   client = test_framework_new_default_client ();
   coll = mongoc_client_get_collection (client, "db", "coll");
   (void) mongoc_collection_drop (coll, NULL);
   schema = get_bson_from_json_file ("./src/libmongoc/tests/"
                                     "client_side_encryption_prose/corpus/"
                                     "corpus-schema.json");
   schema_map = BCON_NEW ("db.coll", BCON_DOCUMENT (schema));
   create_cmd = BCON_NEW ("create", "coll", "validator", "{", "$jsonSchema", BCON_DOCUMENT (schema), "}");

   if (!local_schema) {
      /* Drop and create the collection db.coll configured with the included
       * JSON schema corpus-schema.json */
      res = mongoc_client_command_simple (client, "db", create_cmd, NULL, NULL, &error);
      ASSERT_OR_PRINT (res, error);
   }

   /* Drop the collection keyvault.datakeys. Insert the key documents for each
    * KMS provider. */
   mongoc_collection_destroy (coll);
   coll = mongoc_client_get_collection (client, "keyvault", "datakeys");
   (void) mongoc_collection_drop (coll, NULL);
   wc = mongoc_write_concern_new ();
   mongoc_write_concern_set_wmajority (wc, 1000);
   mongoc_collection_set_write_concern (coll, wc);
   _insert_from_file (coll,
                      "./src/libmongoc/tests/client_side_encryption_prose/"
                      "corpus/corpus-key-aws.json");
   _insert_from_file (coll,
                      "./src/libmongoc/tests/client_side_encryption_prose/"
                      "corpus/corpus-key-azure.json");
   _insert_from_file (coll,
                      "./src/libmongoc/tests/client_side_encryption_prose/"
                      "corpus/corpus-key-gcp.json");
   _insert_from_file (coll,
                      "./src/libmongoc/tests/client_side_encryption_prose/"
                      "corpus/corpus-key-local.json");
   _insert_from_file (coll,
                      "./src/libmongoc/tests/client_side_encryption_prose/"
                      "corpus/corpus-key-kmip.json");

   /* Create a MongoClient configured with auto encryption */
   client_encrypted = test_framework_new_default_client ();
   auto_encryption_opts = mongoc_auto_encryption_opts_new ();
   mongoc_auto_encryption_opts_set_schema_map (auto_encryption_opts, schema_map);
   _check_bypass (auto_encryption_opts);
   kms_providers = _make_kms_providers (true /* aws */, true /* local */);
   mongoc_auto_encryption_opts_set_kms_providers (auto_encryption_opts, kms_providers);
   tls_opts = _make_tls_opts ();
   mongoc_auto_encryption_opts_set_tls_opts (auto_encryption_opts, tls_opts);
   mongoc_auto_encryption_opts_set_keyvault_namespace (auto_encryption_opts, "keyvault", "datakeys");
   res = mongoc_client_enable_auto_encryption (client_encrypted, auto_encryption_opts, &error);
   ASSERT_OR_PRINT (res, error);

   /* Create a ClientEncryption object */
   client_encryption_opts = mongoc_client_encryption_opts_new ();
   mongoc_client_encryption_opts_set_kms_providers (client_encryption_opts, kms_providers);
   mongoc_client_encryption_opts_set_tls_opts (client_encryption_opts, tls_opts);
   mongoc_client_encryption_opts_set_keyvault_namespace (client_encryption_opts, "keyvault", "datakeys");
   mongoc_client_encryption_opts_set_keyvault_client (client_encryption_opts, client);
   client_encryption = mongoc_client_encryption_new (client_encryption_opts, &error);
   ASSERT_OR_PRINT (client_encryption, error);

   corpus = get_bson_from_json_file ("./src/libmongoc/tests/client_side_encryption_prose/corpus/corpus.json");

   /* Try each field individually */
   bson_iter_init (&iter, corpus);
   bson_init (&corpus_copied);
   while (bson_iter_next (&iter)) {
      _corpus_copy_field (client_encryption, &iter, &corpus_copied);
   }

   /* Insert corpus_copied with auto encryption  */
   mongoc_collection_destroy (coll);
   coll = mongoc_client_get_collection (client_encrypted, "db", "coll");
   res = mongoc_collection_insert_one (coll, &corpus_copied, NULL, NULL, &error);
   ASSERT_OR_PRINT (res, error);

   /* Get the automatically decrypted corpus */
   cursor = mongoc_collection_find_with_opts (coll, tmp_bson ("{}"), NULL, NULL);
   BSON_ASSERT (mongoc_cursor_next (cursor, &corpus_decrypted));

   /* It should exactly match corpus. match_bson does a subset match, so match
    * in  both directions */
   assert_match_bson (corpus, corpus_decrypted, false);
   assert_match_bson (corpus_decrypted, corpus, false);
   mongoc_cursor_destroy (cursor);

   /* Load corpus-encrypted.json */
   corpus_encrypted_expected = get_bson_from_json_file ("./src/libmongoc/tests/"
                                                        "client_side_encryption_prose/"
                                                        "corpus/corpus-encrypted.json");
   /* Get the actual encrypted document from unencrypted client */
   mongoc_collection_destroy (coll);
   coll = mongoc_client_get_collection (client, "db", "coll");
   cursor = mongoc_collection_find_with_opts (coll, tmp_bson ("{}"), NULL, NULL);
   BSON_ASSERT (mongoc_cursor_next (cursor, &corpus_encrypted_actual));

   /* Iterate over corpus_encrypted_expected, and check corpus_encrypted_actual
    */
   bson_iter_init (&iter, corpus_encrypted_expected);
   while (bson_iter_next (&iter)) {
      bson_iter_t actual_iter;

      BSON_ASSERT (bson_iter_init_find (&actual_iter, corpus_encrypted_actual, bson_iter_key (&iter)));
      _corpus_check_encrypted (client_encryption, &iter, &actual_iter);
   }

   mongoc_cursor_destroy (cursor);
   bson_destroy (corpus_encrypted_expected);
   bson_destroy (corpus);
   bson_destroy (&corpus_copied);
   mongoc_auto_encryption_opts_destroy (auto_encryption_opts);
   mongoc_client_destroy (client_encrypted);
   mongoc_client_encryption_opts_destroy (client_encryption_opts);
   mongoc_client_encryption_destroy (client_encryption);
   bson_destroy (tls_opts);
   bson_destroy (kms_providers);
   mongoc_write_concern_destroy (wc);
   mongoc_collection_destroy (coll);
   bson_destroy (schema);
   bson_destroy (schema_map);
   bson_destroy (create_cmd);
   mongoc_client_destroy (client);
}

/* Prose Test 6: Corpus Test */
static void
test_corpus (void *unused)
{
   BSON_UNUSED (unused);

   _test_corpus (false /* local schema */);
   _test_corpus (true /* local schema */);
}

/* Begin C driver specific, non-spec tests: */
static void
_reset (mongoc_client_pool_t **pool,
        mongoc_client_t **singled_threaded_client,
        mongoc_client_t **multi_threaded_client,
        mongoc_auto_encryption_opts_t **opts,
        bool recreate)
{
   bson_t *kms_providers;
   mongoc_uri_t *uri;
   bson_t *schema;
   bson_t *schema_map;

   ASSERT (pool);

   mongoc_auto_encryption_opts_destroy (*opts);
   *opts = mongoc_auto_encryption_opts_new ();
   {
      bson_t extra = BSON_INITIALIZER;
      _set_extra_bypass (&extra);
      _set_extra_crypt_shared (&extra);
      mongoc_auto_encryption_opts_set_extra (*opts, &extra);
      bson_destroy (&extra);
   }
   mongoc_auto_encryption_opts_set_keyvault_namespace (*opts, "db", "keyvault");
   kms_providers = _make_kms_providers (false /* aws */, true /* local */);
   mongoc_auto_encryption_opts_set_kms_providers (*opts, kms_providers);
   schema = get_bson_from_json_file ("./src/libmongoc/tests/client_side_encryption_prose/schema.json");
   BSON_ASSERT (schema);
   schema_map = BCON_NEW ("db.coll", BCON_DOCUMENT (schema));
   mongoc_auto_encryption_opts_set_schema_map (*opts, schema_map);

   if (*multi_threaded_client) {
      mongoc_client_pool_push (*pool, *multi_threaded_client);
   }

   mongoc_client_destroy (*singled_threaded_client);
   /* Workaround to hide unnecessary logs per CDRIVER-3322 */
   capture_logs (true);
   mongoc_client_pool_destroy (*pool);
   capture_logs (false);

   if (recreate) {
      mongoc_collection_t *coll;
      bson_t *datakey;
      bson_error_t error;
      mongoc_write_concern_t *wc;

      uri = test_framework_get_uri ();
      *pool = test_framework_client_pool_new_from_uri (uri, NULL);
      test_framework_set_pool_ssl_opts (*pool);
      *singled_threaded_client = test_framework_client_new_from_uri (uri, NULL);
      test_framework_set_ssl_opts (*singled_threaded_client);
      *multi_threaded_client = mongoc_client_pool_pop (*pool);
      mongoc_uri_destroy (uri);

      /* create key */
      coll = mongoc_client_get_collection (*singled_threaded_client, "db", "keyvault");
      (void) mongoc_collection_drop (coll, NULL);
      wc = mongoc_write_concern_new ();
      mongoc_write_concern_set_wmajority (wc, 1000);
      mongoc_collection_set_write_concern (coll, wc);
      datakey = get_bson_from_json_file ("./src/libmongoc/tests/client_side_encryption_prose/limits-key.json");
      BSON_ASSERT (datakey);
      ASSERT_OR_PRINT (mongoc_collection_insert_one (coll, datakey, NULL /* opts */, NULL /* reply */, &error), error);

      bson_destroy (datakey);
      mongoc_write_concern_destroy (wc);
      mongoc_collection_destroy (coll);
   }
   bson_destroy (schema);
   bson_destroy (schema_map);
   bson_destroy (kms_providers);
}

static void
_perform_op (mongoc_client_t *client_encrypted)
{
   bool ret;
   bson_error_t error;
   mongoc_collection_t *coll;

   ASSERT (client_encrypted);

   coll = mongoc_client_get_collection (client_encrypted, "db", "coll");
   ret = mongoc_collection_insert_one (
      coll, tmp_bson ("{'encrypted_string': 'abc'}"), NULL /* opts */, NULL /* reply */, &error);
   ASSERT_OR_PRINT (ret, error);
   mongoc_collection_destroy (coll);
}

static void
_perform_op_pooled (mongoc_client_pool_t *client_pool_encrypted)
{
   mongoc_client_t *client_encrypted;

   ASSERT (client_pool_encrypted);

   client_encrypted = mongoc_client_pool_pop (client_pool_encrypted);
   _perform_op (client_encrypted);
   mongoc_client_pool_push (client_pool_encrypted, client_encrypted);
}

static void
test_invalid_single_and_pool_mismatches (void *unused)
{
   mongoc_client_pool_t *pool = NULL;
   mongoc_client_t *single_threaded_client = NULL;
   mongoc_client_t *multi_threaded_client = NULL;
   mongoc_auto_encryption_opts_t *opts = NULL;
   bson_error_t error;
   bool ret;

   BSON_UNUSED (unused);

   _reset (&pool, &single_threaded_client, &multi_threaded_client, &opts, true);

   /* single threaded client, single threaded setter => ok */
   ret = mongoc_client_enable_auto_encryption (single_threaded_client, opts, &error);
   ASSERT_OR_PRINT (ret, error);
   _perform_op (single_threaded_client);

   /* multi threaded client, single threaded setter => bad */
   ret = mongoc_client_enable_auto_encryption (multi_threaded_client, opts, &error);
   BSON_ASSERT (!ret);
   ASSERT_ERROR_CONTAINS (error,
                          MONGOC_ERROR_CLIENT,
                          MONGOC_ERROR_CLIENT_INVALID_ENCRYPTION_ARG,
                          "Cannot enable auto encryption on a pooled client");

   /* pool - pool setter */
   ret = mongoc_client_pool_enable_auto_encryption (pool, opts, &error);
   ASSERT_OR_PRINT (ret, error);
   _perform_op_pooled (pool);

   /* single threaded client, single threaded key vault client => ok */
   _reset (&pool, &single_threaded_client, &multi_threaded_client, &opts, true);
   mongoc_auto_encryption_opts_set_keyvault_client (opts, single_threaded_client);
   ret = mongoc_client_enable_auto_encryption (single_threaded_client, opts, &error);
   ASSERT_OR_PRINT (ret, error);
   _perform_op (single_threaded_client);

   /* single threaded client, multi threaded key vault client => bad */
   _reset (&pool, &single_threaded_client, &multi_threaded_client, &opts, true);
   mongoc_auto_encryption_opts_set_keyvault_client (opts, multi_threaded_client);
   ret = mongoc_client_enable_auto_encryption (single_threaded_client, opts, &error);
   BSON_ASSERT (!ret);
   ASSERT_ERROR_CONTAINS (error,
                          MONGOC_ERROR_CLIENT,
                          MONGOC_ERROR_CLIENT_INVALID_ENCRYPTION_ARG,
                          "The key vault client must be single threaded, not "
                          "be from a client pool");

   /* single threaded client, pool key vault client => bad */
   _reset (&pool, &single_threaded_client, &multi_threaded_client, &opts, true);
   mongoc_auto_encryption_opts_set_keyvault_client_pool (opts, pool);
   ret = mongoc_client_enable_auto_encryption (single_threaded_client, opts, &error);
   BSON_ASSERT (!ret);
   ASSERT_ERROR_CONTAINS (error,
                          MONGOC_ERROR_CLIENT,
                          MONGOC_ERROR_CLIENT_INVALID_ENCRYPTION_ARG,
                          "The key vault client pool only applies to a client "
                          "pool, not a single threaded client");

   /* pool, singled threaded key vault client => bad */
   _reset (&pool, &single_threaded_client, &multi_threaded_client, &opts, true);
   mongoc_auto_encryption_opts_set_keyvault_client (opts, single_threaded_client);
   ret = mongoc_client_pool_enable_auto_encryption (pool, opts, &error);
   BSON_ASSERT (!ret);
   ASSERT_ERROR_CONTAINS (error,
                          MONGOC_ERROR_CLIENT,
                          MONGOC_ERROR_CLIENT_INVALID_ENCRYPTION_ARG,
                          "The key vault client only applies to a single "
                          "threaded client not a client pool. Set a "
                          "key vault client pool");

   /* pool, multi threaded key vault client => bad */
   _reset (&pool, &single_threaded_client, &multi_threaded_client, &opts, true);
   mongoc_auto_encryption_opts_set_keyvault_client (opts, multi_threaded_client);
   ret = mongoc_client_pool_enable_auto_encryption (pool, opts, &error);
   BSON_ASSERT (!ret);
   ASSERT_ERROR_CONTAINS (error,
                          MONGOC_ERROR_CLIENT,
                          MONGOC_ERROR_CLIENT_INVALID_ENCRYPTION_ARG,
                          "The key vault client only applies to a single "
                          "threaded client not a client pool. Set a "
                          "key vault client pool");

   /* pool, pool key vault client => ok */
   _reset (&pool, &single_threaded_client, &multi_threaded_client, &opts, true);
   mongoc_auto_encryption_opts_set_keyvault_client_pool (opts, pool);
   ret = mongoc_client_pool_enable_auto_encryption (pool, opts, &error);
   ASSERT_OR_PRINT (ret, error);
   _perform_op_pooled (pool);

   /* double enabling */
   _reset (&pool, &single_threaded_client, &multi_threaded_client, &opts, true);
   ret = mongoc_client_enable_auto_encryption (single_threaded_client, opts, &error);
   ASSERT_OR_PRINT (ret, error);
   ret = mongoc_client_enable_auto_encryption (single_threaded_client, opts, &error);
   BSON_ASSERT (!ret);
   ASSERT_ERROR_CONTAINS (
      error, MONGOC_ERROR_CLIENT, MONGOC_ERROR_CLIENT_INVALID_ENCRYPTION_STATE, "Automatic encryption already set");
   ret = mongoc_client_pool_enable_auto_encryption (pool, opts, &error);
   ASSERT_OR_PRINT (ret, error);
   ret = mongoc_client_pool_enable_auto_encryption (pool, opts, &error);
   BSON_ASSERT (!ret);
   ASSERT_ERROR_CONTAINS (
      error, MONGOC_ERROR_CLIENT, MONGOC_ERROR_CLIENT_INVALID_ENCRYPTION_STATE, "Automatic encryption already set");

   /* single threaded, using self as key vault client => redundant, but ok */
   _reset (&pool, &single_threaded_client, &multi_threaded_client, &opts, true);
   mongoc_auto_encryption_opts_set_keyvault_client (opts, single_threaded_client);
   ret = mongoc_client_enable_auto_encryption (single_threaded_client, opts, &error);
   ASSERT_OR_PRINT (ret, error);
   _perform_op (single_threaded_client);

   /* pool, using self as key vault client pool => redundant, but ok */
   _reset (&pool, &single_threaded_client, &multi_threaded_client, &opts, true);
   mongoc_auto_encryption_opts_set_keyvault_client_pool (opts, pool);
   ret = mongoc_client_pool_enable_auto_encryption (pool, opts, &error);
   ASSERT_OR_PRINT (ret, error);
   _perform_op_pooled (pool);

   _reset (&pool, &single_threaded_client, &multi_threaded_client, &opts, false);
   mongoc_auto_encryption_opts_destroy (opts);
}

static BSON_THREAD_FUN (_worker_thread, client_ptr)
{
   mongoc_client_t *client_encrypted;
   mongoc_collection_t *coll;
   mongoc_cursor_t *cursor;
   const bson_t *doc;
   bson_t filter = BSON_INITIALIZER;
   bson_t *to_insert = BCON_NEW ("encrypted_string", "abc");
   int i;
   bool ret;
   bson_error_t error;

   client_encrypted = client_ptr;
   coll = mongoc_client_get_collection (client_encrypted, "db", "coll");

   for (i = 0; i < 100; i++) {
      ret = mongoc_collection_insert_one (coll, to_insert, NULL /* opts */, NULL /* reply */, &error);
      ASSERT_OR_PRINT (ret, error);
      cursor = mongoc_collection_find_with_opts (coll, &filter, NULL /* opts */, NULL /* read_prefs */);
      mongoc_cursor_next (cursor, &doc);
      mongoc_cursor_destroy (cursor);
   }
   mongoc_collection_destroy (coll);
   bson_destroy (&filter);
   bson_destroy (to_insert);
   BSON_THREAD_RETURN;
}

static void
_test_multi_threaded (bool external_key_vault)
{
   /* Spawn two threads and do repeated encryption/decryption operations. */
   bson_thread_t threads[2];
   mongoc_uri_t *uri;
   mongoc_client_pool_t *pool;
   mongoc_client_t *client;
   mongoc_client_t *client1;
   mongoc_client_t *client2;
   mongoc_auto_encryption_opts_t *opts;
   bson_t *datakey;
   mongoc_collection_t *coll;
   bson_t *schema;
   bson_t *schema_map;
   bool ret;
   bson_error_t error;
   bson_t *kms_providers;
   int r;
   int i;
   mongoc_write_concern_t *wc;

   uri = test_framework_get_uri ();
   pool = test_framework_client_pool_new_from_uri (uri, NULL);
   test_framework_set_pool_ssl_opts (pool);
   client = test_framework_client_new_from_uri (uri, NULL);
   test_framework_set_ssl_opts (client);
   opts = mongoc_auto_encryption_opts_new ();

   /* Do setup: create a data key and configure pool for auto encryption. */
   coll = mongoc_client_get_collection (client, "db", "keyvault");
   (void) mongoc_collection_drop (coll, NULL);
   datakey = get_bson_from_json_file ("./src/libmongoc/tests/client_side_encryption_prose/limits-key.json");
   BSON_ASSERT (datakey);
   wc = mongoc_write_concern_new ();
   mongoc_write_concern_set_wmajority (wc, 1000);
   mongoc_collection_set_write_concern (coll, wc);
   ASSERT_OR_PRINT (mongoc_collection_insert_one (coll, datakey, NULL /* opts */, NULL /* reply */, &error), error);

   /* create pool with auto encryption */
   _check_bypass (opts);

   mongoc_auto_encryption_opts_set_keyvault_namespace (opts, "db", "keyvault");
   kms_providers = _make_kms_providers (false /* aws */, true /* local */);
   mongoc_auto_encryption_opts_set_kms_providers (opts, kms_providers);

   if (external_key_vault) {
      mongoc_auto_encryption_opts_set_keyvault_client_pool (opts, pool);
   }

   schema = get_bson_from_json_file ("./src/libmongoc/tests/client_side_encryption_prose/schema.json");
   BSON_ASSERT (schema);
   schema_map = BCON_NEW ("db.coll", BCON_DOCUMENT (schema));
   mongoc_auto_encryption_opts_set_schema_map (opts, schema_map);
   ret = mongoc_client_pool_enable_auto_encryption (pool, opts, &error);
   ASSERT_OR_PRINT (ret, error);

   client1 = mongoc_client_pool_pop (pool);
   client2 = mongoc_client_pool_pop (pool);

   r = mcommon_thread_create (threads, _worker_thread, client1);
   BSON_ASSERT (r == 0);

   r = mcommon_thread_create (threads + 1, _worker_thread, client2);
   BSON_ASSERT (r == 0);

   for (i = 0; i < 2; i++) {
      r = mcommon_thread_join (threads[i]);
      BSON_ASSERT (r == 0);
   }

   mongoc_write_concern_destroy (wc);
   mongoc_collection_destroy (coll);
   mongoc_client_destroy (client);
   mongoc_client_pool_push (pool, client1);
   mongoc_client_pool_push (pool, client2);
   mongoc_client_pool_destroy (pool);
   bson_destroy (schema);
   bson_destroy (schema_map);
   bson_destroy (datakey);
   mongoc_auto_encryption_opts_destroy (opts);
   mongoc_uri_destroy (uri);
   bson_destroy (kms_providers);
}

static void
test_multi_threaded (void *ctx_unused)
{
   BSON_UNUSED (ctx_unused);

   _test_multi_threaded (true);
   _test_multi_threaded (false);
}

static void
test_malformed_explicit (void *unused)
{
   mongoc_client_t *client;
   bson_t *kms_providers;
   mongoc_client_encryption_t *client_encryption;
   mongoc_client_encryption_opts_t *client_encryption_opts;
   bson_value_t value;
   bson_value_t ciphertext;
   bool ret;
   bson_error_t error;

   BSON_UNUSED (unused);

   /* Create a MongoClient without encryption enabled */
   client = test_framework_new_default_client ();
   kms_providers = _make_kms_providers (false /* aws */, true /* local */);

   /* Create a ClientEncryption object */
   client_encryption_opts = mongoc_client_encryption_opts_new ();
   mongoc_client_encryption_opts_set_kms_providers (client_encryption_opts, kms_providers);
   mongoc_client_encryption_opts_set_keyvault_namespace (client_encryption_opts, "keyvault", "datakeys");
   mongoc_client_encryption_opts_set_keyvault_client (client_encryption_opts, client);
   client_encryption = mongoc_client_encryption_new (client_encryption_opts, &error);
   ASSERT_OR_PRINT (client_encryption, error);

   /* Test attempting to decrypt a malformed value */
   ciphertext.value_type = BSON_TYPE_DOUBLE;
   ciphertext.value.v_double = 1.23;
   ret = mongoc_client_encryption_decrypt (client_encryption, &ciphertext, &value, &error);
   BSON_ASSERT (!ret);
   bson_value_destroy (&value);

   mongoc_client_encryption_opts_destroy (client_encryption_opts);
   mongoc_client_encryption_destroy (client_encryption);
   bson_destroy (kms_providers);
   mongoc_client_destroy (client);
}

static void
_check_mongocryptd_not_spawned (void)
{
   mongoc_client_t *client;
   bson_t *cmd;
   bson_error_t error;
   bool ret;

   /* Set up client. */
   {
      mongoc_uri_t *uri = mongoc_uri_new ("mongodb://localhost:27021");
      ASSERT (mongoc_uri_set_option_as_int32 (uri, MONGOC_URI_SERVERSELECTIONTIMEOUTMS, 1000));
      /* Set SERVERSELECTIONTRYONCE to false so client will wait for the full
       * second before giving up on server selection. */
      ASSERT (mongoc_uri_set_option_as_bool (uri, MONGOC_URI_SERVERSELECTIONTRYONCE, false));

      client = mongoc_client_new_from_uri (uri);
      /* Bypass the 5 second cooldown so attempts to connect are repeated.
       * Single threaded clients wait for 5 second cooldown period after failing
       * to connect to a server before connecting again. If mongocryptd just
       * spawned, it may take time before connections are accepted. */
      _mongoc_topology_bypass_cooldown (client->topology);
      mongoc_uri_destroy (uri);
   }
   cmd = BCON_NEW (HANDSHAKE_CMD_LEGACY_HELLO, BCON_INT32 (1));
   ret = mongoc_client_command_simple (client, "keyvault", cmd, NULL /* read prefs */, NULL /* reply */, &error);
   BSON_ASSERT (!ret);
   ASSERT_ERROR_CONTAINS (
      error, MONGOC_ERROR_SERVER_SELECTION, MONGOC_ERROR_SERVER_SELECTION_FAILURE, "No suitable servers");
   mongoc_client_destroy (client);
   bson_destroy (cmd);
}

/* Prose Test 8: Bypass Spawning mongocryptd - Via mongocryptdBypassSpawn */
static void
test_bypass_spawning_via_mongocryptdBypassSpawn (void *unused)
{
   mongoc_client_t *client_encrypted;
   mongoc_auto_encryption_opts_t *auto_encryption_opts;
   bson_t *kms_providers;
   bson_t *doc_to_insert;
   bson_t *extra;
   bson_t *schema_map;
   bson_t *schema;
   bool ret;
   bson_error_t error;
   mongoc_collection_t *coll;

   BSON_UNUSED (unused);

   auto_encryption_opts = mongoc_auto_encryption_opts_new ();
   kms_providers = _make_kms_providers (false /* aws */, true /* local */);
   mongoc_auto_encryption_opts_set_kms_providers (auto_encryption_opts, kms_providers);
   mongoc_auto_encryption_opts_set_keyvault_namespace (auto_encryption_opts, "keyvault", "datakeys");
   schema = get_bson_from_json_file ("./src/libmongoc/tests/"
                                     "client_side_encryption_prose/external/"
                                     "external-schema.json");
   schema_map = BCON_NEW ("db.coll", BCON_DOCUMENT (schema));

   /* Create a MongoClient with encryption enabled */
   client_encrypted = test_framework_new_default_client ();
   extra = BCON_NEW ("mongocryptdBypassSpawn",
                     BCON_BOOL (true),
                     "mongocryptdSpawnArgs",
                     "[",
                     "--pidfilepath=bypass-spawning-mongocryptd.pid",
                     "--port=27021",
                     "]",
                     "mongocryptdURI",
                     "mongodb://localhost:27021/?serverSelectionTimeoutMS=1000");
   mongoc_auto_encryption_opts_set_extra (auto_encryption_opts, extra);
   mongoc_auto_encryption_opts_set_schema_map (auto_encryption_opts, schema_map);
   bson_destroy (extra);
   ret = mongoc_client_enable_auto_encryption (client_encrypted, auto_encryption_opts, &error);
   ASSERT_OR_PRINT (ret, error);

   /* Insert { 'encrypt': 'test' }. Should fail with a server selection error.
    */
   coll = mongoc_client_get_collection (client_encrypted, "db", "coll");
   doc_to_insert = BCON_NEW ("encrypt", "test");
   ret = mongoc_collection_insert_one (coll, doc_to_insert, NULL /* opts */, NULL /* reply */, &error);
   BSON_ASSERT (!ret);
   ASSERT_ERROR_CONTAINS (error,
                          MONGOC_ERROR_SERVER_SELECTION,
                          MONGOC_ERROR_SERVER_SELECTION_FAILURE,
                          "mongocryptd error: No suitable servers found");

   _check_mongocryptd_not_spawned ();
   bson_destroy (schema_map);
   bson_destroy (schema);
   mongoc_collection_destroy (coll);
   mongoc_client_destroy (client_encrypted);
   bson_destroy (doc_to_insert);
   mongoc_auto_encryption_opts_destroy (auto_encryption_opts);
   bson_destroy (kms_providers);
}

static void
test_bypass_spawning_via_helper (const char *auto_encryption_opt)
{
   mongoc_client_t *client_encrypted;
   mongoc_auto_encryption_opts_t *auto_encryption_opts;
   bson_t *kms_providers;
   bson_t *doc_to_insert;
   bson_t *extra = bson_new ();
   bool ret;
   bson_error_t error;
   bool check_crypt_shared = false;
   mongoc_collection_t *coll;

   auto_encryption_opts = mongoc_auto_encryption_opts_new ();
   kms_providers = _make_kms_providers (false /* aws */, true /* local */);
   mongoc_auto_encryption_opts_set_kms_providers (auto_encryption_opts, kms_providers);
   mongoc_auto_encryption_opts_set_keyvault_namespace (auto_encryption_opts, "keyvault", "datakeys");
   if (0 == strcmp (auto_encryption_opt, "bypass_auto_encryption")) {
      mongoc_auto_encryption_opts_set_bypass_auto_encryption (auto_encryption_opts, true);
   } else if (0 == strcmp (auto_encryption_opt, "bypass_query_analysis")) {
      mongoc_auto_encryption_opts_set_bypass_query_analysis (auto_encryption_opts, true);
   } else if (0 == strcmp (auto_encryption_opt, "cryptSharedLibRequired")) {
      bson_t *schema = get_bson_from_json_file ("./src/libmongoc/tests/"
                                                "client_side_encryption_prose/external/"
                                                "external-schema.json");
      BSON_ASSERT (schema);
      bson_t *schema_map = BCON_NEW ("db.coll", BCON_DOCUMENT (schema));
      mongoc_auto_encryption_opts_set_schema_map (auto_encryption_opts, schema_map);
      check_crypt_shared = true;
      char *env_cryptSharedLibPath = test_framework_getenv ("MONGOC_TEST_CRYPT_SHARED_LIB_PATH");
      BSON_ASSERT (env_cryptSharedLibPath);
      BSON_APPEND_UTF8 (extra, "cryptSharedLibPath", env_cryptSharedLibPath);
      BSON_APPEND_BOOL (extra, "cryptSharedLibRequired", true);
      BSON_APPEND_UTF8 (extra, "mongocryptdURI", "mongodb://localhost:27021/db?serverSelectionTimeoutMS=1000");
      bson_free (env_cryptSharedLibPath);
      bson_destroy (schema);
      bson_destroy (schema_map);
   } else {
      test_error ("Unexpected 'auto_encryption_opt' argument: %s", auto_encryption_opt);
   }

   /* Create a MongoClient with encryption enabled */
   client_encrypted = test_framework_new_default_client ();
   BCON_APPEND (
      extra, "mongocryptdSpawnArgs", "[", "--pidfilepath=bypass-spawning-mongocryptd.pid", "--port=27021", "]");
   mongoc_auto_encryption_opts_set_extra (auto_encryption_opts, extra);
   bson_destroy (extra);
   ret = mongoc_client_enable_auto_encryption (client_encrypted, auto_encryption_opts, &error);
   ASSERT_OR_PRINT (ret, error);

   if (check_crypt_shared) {
      BSON_ASSERT (mongoc_client_get_crypt_shared_version (client_encrypted) != NULL);
   }

   /* Insert { 'unencrypted': 'test' }. Should succeed. */
   coll = mongoc_client_get_collection (client_encrypted, "db", "coll");
   doc_to_insert = BCON_NEW ("unencrypted", "test");
   ret = mongoc_collection_insert_one (coll, doc_to_insert, NULL /* opts */, NULL /* reply */, &error);
   ASSERT_OR_PRINT (ret, error);

   _check_mongocryptd_not_spawned ();

   mongoc_collection_destroy (coll);
   mongoc_client_destroy (client_encrypted);
   bson_destroy (doc_to_insert);
   mongoc_auto_encryption_opts_destroy (auto_encryption_opts);
   bson_destroy (kms_providers);
}

/* Prose Test 8: Bypass Spawning mongocryptd - Via bypassAutoEncryption */
static void
test_bypass_spawning_via_bypassAutoEncryption (void *unused)
{
   BSON_UNUSED (unused);

   test_bypass_spawning_via_helper ("bypass_auto_encryption");
}

/* Prose Test 8: Bypass Spawning mongocryptd - Via bypassQueryAnalysis */
static void
test_bypass_spawning_via_bypassQueryAnalysis (void *unused)
{
   BSON_UNUSED (unused);

   test_bypass_spawning_via_helper ("bypass_query_analysis");
}

/* Prose Test 8: Bypass Spawning mongocryptd - Via loading shared library */
static void
test_bypass_spawning_via_cryptSharedLibLoaded (void *unused)
{
   BSON_UNUSED (unused);
   test_bypass_spawning_via_helper ("cryptSharedLibRequired");
}

static int
_skip_if_no_crypt_shared (void)
{
   char *env = test_framework_getenv ("MONGOC_TEST_CRYPT_SHARED_LIB_PATH");
   if (!env) {
      return 0; // Skip!
   }
   bson_free (env);
   return 1; // Do not skip
}

static mongoc_client_encryption_t *
_make_kms_certificate_client_encryption (mongoc_client_t *client, bson_error_t *error)
{
   mongoc_client_encryption_t *client_encryption;

   ASSERT (client);

   mongoc_client_encryption_opts_t *client_encryption_opts = mongoc_client_encryption_opts_new ();

   {
      bson_t *kms_providers = _make_aws_kms_provider (NULL);
      mongoc_client_encryption_opts_set_kms_providers (client_encryption_opts, kms_providers);
      bson_destroy (kms_providers);
   }

   {
      char *tls_ca_file = test_framework_getenv_required ("MONGOC_TEST_CSFLE_TLS_CA_FILE");
      char *tls_cert_key_file = test_framework_getenv_required ("MONGOC_TEST_CSFLE_TLS_CERTIFICATE_KEY_FILE");
      bson_t *tls_opts = tmp_bson ("{ 'aws': { '%s': '%s', '%s': '%s' } }",
                                   MONGOC_URI_TLSCAFILE,
                                   tls_ca_file,
                                   MONGOC_URI_TLSCERTIFICATEKEYFILE,
                                   tls_cert_key_file);

      mongoc_client_encryption_opts_set_tls_opts (client_encryption_opts, tls_opts);

      bson_free (tls_cert_key_file);
      bson_free (tls_ca_file);
   }

   mongoc_client_encryption_opts_set_keyvault_namespace (client_encryption_opts, "keyvault", "datakeys");
   mongoc_client_encryption_opts_set_keyvault_client (client_encryption_opts, client);

   client_encryption = mongoc_client_encryption_new (client_encryption_opts, error);
   ASSERT_OR_PRINT (client_encryption, (*error));

   mongoc_client_encryption_opts_destroy (client_encryption_opts);
   return client_encryption;
}


static void
test_kms_tls_cert_valid (void *unused)
{
   const int32_t connecttimeoutms = MONGOC_DEFAULT_CONNECTTIMEOUTMS;
   const int is_client = 1;

   bson_error_t error;
   mongoc_host_list_t host;
   mongoc_stream_t *base_stream;
   mongoc_ssl_opt_t ssl_opts;
   mongoc_stream_t *tls_stream;

   char *tls_ca_file = test_framework_getenv_required ("MONGOC_TEST_CSFLE_TLS_CA_FILE");
   char *tls_cert_key_file = test_framework_getenv_required ("MONGOC_TEST_CSFLE_TLS_CERTIFICATE_KEY_FILE");

   BSON_UNUSED (unused);

#if defined(MONGOC_ENABLE_SSL_SECURE_CHANNEL)
   /* Certificate verification fails with Secure Channel given
    * "127.0.0.1:8999" with error: "hostname doesn't match certificate". */
   ASSERT_OR_PRINT (_mongoc_host_list_from_string_with_err (&host, "localhost:8999", &error), error);
#else
   ASSERT_OR_PRINT (_mongoc_host_list_from_string_with_err (&host, "127.0.0.1:8999", &error), error);
#endif

   base_stream = mongoc_client_connect_tcp (connecttimeoutms, &host, &error);
   ASSERT_OR_PRINT (base_stream, error);

   ssl_opts = *test_framework_get_ssl_opts ();
   ssl_opts.ca_file = tls_ca_file;
   ssl_opts.pem_file = tls_cert_key_file;

   tls_stream = mongoc_stream_tls_new_with_hostname (base_stream, host.host, &ssl_opts, is_client);

   ASSERT_OR_PRINT (mongoc_stream_tls_handshake_block (tls_stream, host.host, connecttimeoutms, &error), error);

   mongoc_stream_destroy (tls_stream); /* Also destroys base_stream. */

   bson_free (tls_cert_key_file);
   bson_free (tls_ca_file);
}

/* Prose Test 10: KMS TLS Tests - Invalid KMS Certificate */
static void
test_kms_tls_cert_expired (void *unused)
{
   bool ret;

   bson_error_t error;

   bson_value_t keyid;

   mongoc_client_t *client = test_framework_new_default_client ();

   mongoc_client_encryption_t *client_encryption = _make_kms_certificate_client_encryption (client, &error);

   mongoc_client_encryption_datakey_opts_t *opts = mongoc_client_encryption_datakey_opts_new ();

   BSON_UNUSED (unused);

   mongoc_client_encryption_datakey_opts_set_masterkey (opts,
                                                        tmp_bson ("{ 'region': 'us-east-1', 'key': "
                                                                  "'arn:aws:kms:us-east-1:579766882180:key/"
                                                                  "89fcc2c4-08b0-4bd9-9f25-e30687b580d0', "
                                                                  "'endpoint': '127.0.0.1:9000' }"));

   ret = mongoc_client_encryption_create_datakey (client_encryption, "aws", opts, &keyid, &error);

   BSON_ASSERT (!ret);

#if defined(MONGOC_ENABLE_SSL_OPENSSL)
   ASSERT_CONTAINS (error.message, "certificate has expired");
#elif defined(MONGOC_ENABLE_SSL_SECURE_TRANSPORT)
   ASSERT_CONTAINS (error.message, "CSSMERR_TP_CERT_EXPIRED");
#elif defined(MONGOC_ENABLE_SSL_SECURE_CHANNEL)
   ASSERT_CONTAINS (error.message, "certificate has expired");
#elif defined(MONGOC_ENABLE_SSL_LIBRESSL)
   ASSERT_CONTAINS (error.message, "certificate has expired");
#endif

   mongoc_client_encryption_datakey_opts_destroy (opts);
   mongoc_client_encryption_destroy (client_encryption);
   mongoc_client_destroy (client);
}


/* Prose Test 10: KMS TLS Tests - Invalid Hostname in KMS Certificate */
static void
test_kms_tls_cert_wrong_host (void *unused)
{
   bool ret;

   bson_error_t error;

   bson_value_t keyid;

   mongoc_client_t *client = test_framework_new_default_client ();

   mongoc_client_encryption_t *client_encryption = _make_kms_certificate_client_encryption (client, &error);

   mongoc_client_encryption_datakey_opts_t *opts = mongoc_client_encryption_datakey_opts_new ();

   BSON_UNUSED (unused);

   mongoc_client_encryption_datakey_opts_set_masterkey (opts,
                                                        tmp_bson ("{ 'region': 'us-east-1', 'key': "
                                                                  "'arn:aws:kms:us-east-1:579766882180:key/"
                                                                  "89fcc2c4-08b0-4bd9-9f25-e30687b580d0', "
                                                                  "'endpoint': '127.0.0.1:9001' }"));

   ret = mongoc_client_encryption_create_datakey (client_encryption, "aws", opts, &keyid, &error);

   BSON_ASSERT (!ret);

#if defined(MONGOC_ENABLE_SSL_OPENSSL)
   ASSERT_CONTAINS (error.message, "IP address mismatch");
#elif defined(MONGOC_ENABLE_SSL_SECURE_TRANSPORT)
   ASSERT_CONTAINS (error.message, "Host name mismatch");
#elif defined(MONGOC_ENABLE_SSL_SECURE_CHANNEL)
   ASSERT_CONTAINS (error.message, "hostname doesn't match certificate");
#elif defined(MONGOC_ENABLE_SSL_LIBRESSL)
   ASSERT_CONTAINS (error.message, "not present in server certificate");
#endif

   mongoc_client_encryption_datakey_opts_destroy (opts);
   mongoc_client_encryption_destroy (client_encryption);
   mongoc_client_destroy (client);
}

typedef enum { NO_CLIENT_CERT, WITH_TLS, INVALID_HOSTNAME, EXPIRED, WITH_NAMES } tls_test_ce_t;

static mongoc_client_encryption_t *
_tls_test_make_client_encryption (mongoc_client_t *keyvault_client, tls_test_ce_t test_ce)
{
   bson_t *kms_providers;
   mongoc_client_encryption_opts_t *client_encryption_opts;
   bson_error_t error = {0};
   mongoc_client_encryption_t *client_encryption;
   bson_t *tls_opts = NULL;

   ASSERT (keyvault_client);

   char *mongoc_test_aws_access_key_id = test_framework_getenv_required ("MONGOC_TEST_AWS_ACCESS_KEY_ID");
   char *mongoc_test_aws_secret_access_key = test_framework_getenv_required ("MONGOC_TEST_AWS_SECRET_ACCESS_KEY");
   char *mongoc_test_azure_tenant_id = test_framework_getenv_required ("MONGOC_TEST_AZURE_TENANT_ID");
   char *mongoc_test_azure_client_id = test_framework_getenv_required ("MONGOC_TEST_AZURE_CLIENT_ID");
   char *mongoc_test_azure_client_secret = test_framework_getenv_required ("MONGOC_TEST_AZURE_CLIENT_SECRET");
   char *mongoc_test_gcp_email = test_framework_getenv_required ("MONGOC_TEST_GCP_EMAIL");
   char *mongoc_test_gcp_privatekey = test_framework_getenv_required ("MONGOC_TEST_GCP_PRIVATEKEY");
   char *ca_file = test_framework_getenv_required ("MONGOC_TEST_CSFLE_TLS_CA_FILE");
   char *certificate_key_file = test_framework_getenv_required ("MONGOC_TEST_CSFLE_TLS_CERTIFICATE_KEY_FILE");

   if (test_ce == WITH_TLS) {
      kms_providers = tmp_bson ("{'aws': {'accessKeyId': '%s', 'secretAccessKey': '%s' }}",
                                mongoc_test_aws_access_key_id,
                                mongoc_test_aws_secret_access_key);

      tls_opts =
         tmp_bson ("{'aws': {'tlsCaFile': '%s', 'tlsCertificateKeyFile': '%s' }}", ca_file, certificate_key_file);
      bson_concat (kms_providers,
                   tmp_bson ("{'azure': {'tenantId': '%s', 'clientId': '%s', "
                             "'clientSecret': '%s', "
                             "'identityPlatformEndpoint': '127.0.0.1:9002' }}",
                             mongoc_test_azure_tenant_id,
                             mongoc_test_azure_client_id,
                             mongoc_test_azure_client_secret));
      bson_concat (
         tls_opts,
         tmp_bson ("{'azure': {'tlsCaFile': '%s', 'tlsCertificateKeyFile': '%s' }}", ca_file, certificate_key_file));

      bson_concat (kms_providers,
                   tmp_bson ("{'gcp': { 'email': '%s', 'privateKey': '%s', "
                             "'endpoint': '127.0.0.1:9002' }}",
                             mongoc_test_gcp_email,
                             mongoc_test_gcp_privatekey));
      bson_concat (
         tls_opts,
         tmp_bson ("{'gcp': {'tlsCaFile': '%s', 'tlsCertificateKeyFile': '%s' }}", ca_file, certificate_key_file));

      bson_concat (kms_providers, tmp_bson ("{'kmip': { 'endpoint': '127.0.0.1:5698'}}"));
      bson_concat (
         tls_opts,
         tmp_bson ("{'kmip': {'tlsCaFile': '%s', 'tlsCertificateKeyFile': '%s' }}", ca_file, certificate_key_file));
   } else if (test_ce == NO_CLIENT_CERT) {
      kms_providers = tmp_bson ("{'aws': {'accessKeyId': '%s', 'secretAccessKey': '%s' }}",
                                mongoc_test_aws_access_key_id,
                                mongoc_test_aws_secret_access_key);
      bson_concat (kms_providers,
                   tmp_bson ("{'azure': {'tenantId': '%s', 'clientId': '%s', "
                             "'clientSecret': '%s', "
                             "'identityPlatformEndpoint': '127.0.0.1:9002'}}",
                             mongoc_test_azure_tenant_id,
                             mongoc_test_azure_client_id,
                             mongoc_test_azure_client_secret));
      bson_concat (kms_providers,
                   tmp_bson ("{'gcp': { 'email': '%s', 'privateKey': '%s', "
                             "'endpoint': '127.0.0.1:9002'}}",
                             mongoc_test_gcp_email,
                             mongoc_test_gcp_privatekey));
      bson_concat (kms_providers, tmp_bson ("{'kmip': { 'endpoint': '127.0.0.1:5698' }}"));
   } else if (test_ce == EXPIRED) {
      kms_providers = tmp_bson ("{'aws': {'accessKeyId': '%s', 'secretAccessKey': '%s' }}",
                                mongoc_test_aws_access_key_id,
                                mongoc_test_aws_secret_access_key);
      tls_opts = tmp_bson ("{'aws': {'tlsCaFile': '%s'} }", ca_file);

      bson_concat (kms_providers,
                   tmp_bson ("{'azure': {'tenantId': '%s', 'clientId': '%s', "
                             "'clientSecret': '%s', "
                             "'identityPlatformEndpoint': '127.0.0.1:9000'}}",
                             mongoc_test_azure_tenant_id,
                             mongoc_test_azure_client_id,
                             mongoc_test_azure_client_secret));
      bson_concat (tls_opts, tmp_bson ("{'azure': {'tlsCaFile': '%s'} }", ca_file));

      bson_concat (kms_providers,
                   tmp_bson ("{'gcp': { 'email': '%s', 'privateKey': '%s', "
                             "'endpoint': '127.0.0.1:9000'}}",
                             mongoc_test_gcp_email,
                             mongoc_test_gcp_privatekey));
      bson_concat (tls_opts, tmp_bson ("{'gcp': {'tlsCaFile': '%s'} }", ca_file));

      bson_concat (kms_providers, tmp_bson ("{'kmip': { 'endpoint': '127.0.0.1:9000' }}"));
      bson_concat (tls_opts, tmp_bson ("{'kmip': {'tlsCaFile': '%s'} }", ca_file));
   } else if (test_ce == INVALID_HOSTNAME) {
      kms_providers = tmp_bson ("{'aws': {'accessKeyId': '%s', 'secretAccessKey': '%s' } }",
                                mongoc_test_aws_access_key_id,
                                mongoc_test_aws_secret_access_key);
      tls_opts = tmp_bson ("{'aws': {'tlsCaFile': '%s'} }", ca_file);

      bson_concat (kms_providers,
                   tmp_bson ("{'azure': {'tenantId': '%s', 'clientId': '%s', "
                             "'clientSecret': '%s', "
                             "'identityPlatformEndpoint': '127.0.0.1:9001' }}",
                             mongoc_test_azure_tenant_id,
                             mongoc_test_azure_client_id,
                             mongoc_test_azure_client_secret));
      bson_concat (tls_opts, tmp_bson ("{'azure': {'tlsCaFile': '%s'} }", ca_file));

      bson_concat (kms_providers,
                   tmp_bson ("{'gcp': { 'email': '%s', 'privateKey': '%s', "
                             "'endpoint': '127.0.0.1:9001' }}",
                             mongoc_test_gcp_email,
                             mongoc_test_gcp_privatekey));
      bson_concat (tls_opts, tmp_bson ("{'gcp': {'tlsCaFile': '%s'} }", ca_file));

      bson_concat (kms_providers, tmp_bson ("{'kmip': { 'endpoint': '127.0.0.1:9001' }}"));
      bson_concat (tls_opts, tmp_bson ("{'kmip': {'tlsCaFile': '%s'} }", ca_file));
   } else if (test_ce == WITH_NAMES) {
      kms_providers = tmp_bson ("{'aws:no_client_cert': {'accessKeyId': '%s', "
                                "'secretAccessKey': '%s' }}",
                                mongoc_test_aws_access_key_id,
                                mongoc_test_aws_secret_access_key);
      tls_opts = tmp_bson ("{'aws:no_client_cert': {'tlsCaFile': '%s' }}", ca_file);

      bson_concat (kms_providers,
                   tmp_bson ("{'aws:with_tls': {'accessKeyId': '%s', 'secretAccessKey': '%s' }}",
                             mongoc_test_aws_access_key_id,
                             mongoc_test_aws_secret_access_key));
      bson_concat (tls_opts,
                   tmp_bson ("{'aws:with_tls': {'tlsCaFile': '%s', "
                             "'tlsCertificateKeyFile': '%s' }}",
                             ca_file,
                             certificate_key_file));

      bson_concat (kms_providers,
                   tmp_bson ("{'azure:no_client_cert': {'tenantId': '%s', 'clientId': '%s',"
                             "'clientSecret': '%s', "
                             "'identityPlatformEndpoint': '127.0.0.1:9002'"
                             "}}",
                             mongoc_test_azure_tenant_id,
                             mongoc_test_azure_client_id,
                             mongoc_test_azure_client_secret));
      bson_concat (tls_opts, tmp_bson ("{'azure:no_client_cert': {'tlsCaFile': '%s' }}", ca_file));

      bson_concat (kms_providers,
                   tmp_bson ("{'azure:with_tls': {'tenantId': '%s', 'clientId': '%s',"
                             "'clientSecret': '%s', "
                             "'identityPlatformEndpoint': '127.0.0.1:9002'"
                             "}}",
                             mongoc_test_azure_tenant_id,
                             mongoc_test_azure_client_id,
                             mongoc_test_azure_client_secret));
      bson_concat (tls_opts,
                   tmp_bson ("{'azure:with_tls': {'tlsCaFile': '%s', "
                             "'tlsCertificateKeyFile': '%s' }}",
                             ca_file,
                             certificate_key_file));

      bson_concat (kms_providers,
                   tmp_bson ("{'gcp:no_client_cert': { 'email': '%s', 'privateKey': '%s', "
                             "'endpoint': '127.0.0.1:9002' }}",
                             mongoc_test_gcp_email,
                             mongoc_test_gcp_privatekey));
      bson_concat (tls_opts, tmp_bson ("{'gcp:no_client_cert': {'tlsCaFile': '%s' }}", ca_file, certificate_key_file));

      bson_concat (kms_providers,
                   tmp_bson ("{'gcp:with_tls': { 'email': '%s', 'privateKey': '%s', "
                             "'endpoint': '127.0.0.1:9002' }}",
                             mongoc_test_gcp_email,
                             mongoc_test_gcp_privatekey));
      bson_concat (tls_opts,
                   tmp_bson ("{'gcp:with_tls': {'tlsCaFile': '%s', "
                             "'tlsCertificateKeyFile': '%s' }}",
                             ca_file,
                             certificate_key_file));

      bson_concat (kms_providers, tmp_bson ("{'kmip:no_client_cert': { 'endpoint': '127.0.0.1:5698'}}"));
      bson_concat (tls_opts, tmp_bson ("{'kmip:no_client_cert': {'tlsCaFile': '%s' }}", ca_file));

      bson_concat (kms_providers, tmp_bson ("{'kmip:with_tls': { 'endpoint': '127.0.0.1:5698'}}"));
      bson_concat (tls_opts,
                   tmp_bson ("{'kmip:with_tls': {'tlsCaFile': '%s', "
                             "'tlsCertificateKeyFile': '%s' }}",
                             ca_file,
                             certificate_key_file));
   } else {
      BSON_UNREACHABLE ("Invalid value for test_ce");
   }

   client_encryption_opts = mongoc_client_encryption_opts_new ();
   mongoc_client_encryption_opts_set_kms_providers (client_encryption_opts, kms_providers);
   mongoc_client_encryption_opts_set_keyvault_namespace (client_encryption_opts, "keyvault", "datakeys");
   mongoc_client_encryption_opts_set_keyvault_client (client_encryption_opts, keyvault_client);
   mongoc_client_encryption_opts_set_tls_opts (client_encryption_opts, tls_opts);

   client_encryption = mongoc_client_encryption_new (client_encryption_opts, &error);
   ASSERT_OR_PRINT (client_encryption, error);

   mongoc_client_encryption_opts_destroy (client_encryption_opts);
   bson_free (mongoc_test_aws_access_key_id);
   bson_free (mongoc_test_aws_secret_access_key);
   bson_free (mongoc_test_azure_tenant_id);
   bson_free (mongoc_test_azure_client_id);
   bson_free (mongoc_test_azure_client_secret);
   bson_free (mongoc_test_gcp_email);
   bson_free (mongoc_test_gcp_privatekey);
   bson_free (ca_file);
   bson_free (certificate_key_file);

   return client_encryption;
}

#if defined(MONGOC_ENABLE_SSL_OPENSSL)
#define ASSERT_EXPIRED(error) ASSERT_CONTAINS (error.message, "certificate has expired")
#elif defined(MONGOC_ENABLE_SSL_SECURE_TRANSPORT)
#define ASSERT_EXPIRED(error) ASSERT_CONTAINS (error.message, "CSSMERR_TP_CERT_EXPIRED")
#elif defined(MONGOC_ENABLE_SSL_SECURE_CHANNEL)
#define ASSERT_EXPIRED(error) ASSERT_CONTAINS (error.message, "certificate has expired")
#elif defined(MONGOC_ENABLE_SSL_LIBRESSL)
#define ASSERT_EXPIRED(error) ASSERT_CONTAINS (error.message, "certificate has expired")
#else
#define ASSERT_EXPIRED(error)
#endif

#if defined(MONGOC_ENABLE_SSL_OPENSSL)
#define ASSERT_INVALID_HOSTNAME(error) ASSERT_CONTAINS (error.message, "IP address mismatch")
#elif defined(MONGOC_ENABLE_SSL_SECURE_TRANSPORT)
#define ASSERT_INVALID_HOSTNAME(error) ASSERT_CONTAINS (error.message, "Host name mismatch")
#elif defined(MONGOC_ENABLE_SSL_SECURE_CHANNEL)
#define ASSERT_INVALID_HOSTNAME(error) ASSERT_CONTAINS (error.message, "hostname doesn't match certificate")
#elif defined(MONGOC_ENABLE_SSL_LIBRESSL)
#define ASSERT_INVALID_HOSTNAME(error) ASSERT_CONTAINS (error.message, "not present in server certificate")
#else
#define ASSERT_INVALID_HOSTNAME(error)
#endif

/* Prose Test 11: KMS TLS Options Tests */
static void
test_kms_tls_options (void *unused)
{
   mongoc_client_t *keyvault_client;
   mongoc_client_encryption_t *client_encryption_no_client_cert = NULL;
   mongoc_client_encryption_t *client_encryption_with_tls = NULL;
   mongoc_client_encryption_t *client_encryption_expired = NULL;
   mongoc_client_encryption_t *client_encryption_invalid_hostname = NULL;
   mongoc_client_encryption_t *client_encryption_with_names = NULL;
   bson_value_t keyid;
   mongoc_client_encryption_datakey_opts_t *dkopts;
   bson_error_t error;
   bool res;
   const int mongocrypt_errno = 1; /* libmongocrypt returns all errors with code 1. */

   BSON_UNUSED (unused);

   keyvault_client = test_framework_new_default_client ();
   client_encryption_no_client_cert = _tls_test_make_client_encryption (keyvault_client, NO_CLIENT_CERT);
   client_encryption_with_tls = _tls_test_make_client_encryption (keyvault_client, WITH_TLS);
   client_encryption_expired = _tls_test_make_client_encryption (keyvault_client, EXPIRED);
   client_encryption_invalid_hostname = _tls_test_make_client_encryption (keyvault_client, INVALID_HOSTNAME);
   client_encryption_with_names = _tls_test_make_client_encryption (keyvault_client, WITH_NAMES);

   /* Case 1: AWS - no client cert. */
   memset (&error, 0, sizeof (bson_error_t));
   dkopts = mongoc_client_encryption_datakey_opts_new ();
   mongoc_client_encryption_datakey_opts_set_masterkey (dkopts,
                                                        tmp_bson ("{ 'region': 'us-east-1', 'key': "
                                                                  "'arn:aws:kms:us-east-1:579766882180:key/"
                                                                  "89fcc2c4-08b0-4bd9-9f25-e30687b580d0', 'endpoint': "
                                                                  "'127.0.0.1:9002' }"));
   res = mongoc_client_encryption_create_datakey (client_encryption_no_client_cert, "aws", dkopts, &keyid, &error);
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_SOCKET, "");
   ASSERT (!res);
   mongoc_client_encryption_datakey_opts_destroy (dkopts);

   /* AWS - named with no client cert. */
   memset (&error, 0, sizeof (bson_error_t));
   dkopts = mongoc_client_encryption_datakey_opts_new ();
   mongoc_client_encryption_datakey_opts_set_masterkey (dkopts,
                                                        tmp_bson ("{ 'region': 'us-east-1', 'key': "
                                                                  "'arn:aws:kms:us-east-1:579766882180:key/"
                                                                  "89fcc2c4-08b0-4bd9-9f25-e30687b580d0', 'endpoint': "
                                                                  "'127.0.0.1:9002' }"));
   res = mongoc_client_encryption_create_datakey (
      client_encryption_with_names, "aws:no_client_cert", dkopts, &keyid, &error);
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_SOCKET, "");
   ASSERT (!res);
   mongoc_client_encryption_datakey_opts_destroy (dkopts);

   /* AWS - with TLS. */
   memset (&error, 0, sizeof (bson_error_t));
   dkopts = mongoc_client_encryption_datakey_opts_new ();
   mongoc_client_encryption_datakey_opts_set_masterkey (dkopts,
                                                        tmp_bson ("{ 'region': 'us-east-1', 'key': "
                                                                  "'arn:aws:kms:us-east-1:579766882180:key/"
                                                                  "89fcc2c4-08b0-4bd9-9f25-e30687b580d0', 'endpoint': "
                                                                  "'127.0.0.1:9002' }"));
   res = mongoc_client_encryption_create_datakey (client_encryption_with_tls, "aws", dkopts, &keyid, &error);
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_CLIENT_SIDE_ENCRYPTION, mongocrypt_errno, "parse error");
   ASSERT (!res);
   mongoc_client_encryption_datakey_opts_destroy (dkopts);

   /* AWS - named with TLS. */
   memset (&error, 0, sizeof (bson_error_t));
   dkopts = mongoc_client_encryption_datakey_opts_new ();
   mongoc_client_encryption_datakey_opts_set_masterkey (dkopts,
                                                        tmp_bson ("{ 'region': 'us-east-1', 'key': "
                                                                  "'arn:aws:kms:us-east-1:579766882180:key/"
                                                                  "89fcc2c4-08b0-4bd9-9f25-e30687b580d0', 'endpoint': "
                                                                  "'127.0.0.1:9002' }"));
   res = mongoc_client_encryption_create_datakey (client_encryption_with_names, "aws:with_tls", dkopts, &keyid, &error);
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_CLIENT_SIDE_ENCRYPTION, mongocrypt_errno, "parse error");
   ASSERT (!res);
   mongoc_client_encryption_datakey_opts_destroy (dkopts);

   /* AWS - expired. */
   memset (&error, 0, sizeof (bson_error_t));
   dkopts = mongoc_client_encryption_datakey_opts_new ();
   mongoc_client_encryption_datakey_opts_set_masterkey (dkopts,
                                                        tmp_bson ("{ 'region': 'us-east-1', 'key': "
                                                                  "'arn:aws:kms:us-east-1:579766882180:key/"
                                                                  "89fcc2c4-08b0-4bd9-9f25-e30687b580d0', 'endpoint': "
                                                                  "'127.0.0.1:9000' }"));
   res = mongoc_client_encryption_create_datakey (client_encryption_expired, "aws", dkopts, &keyid, &error);
   ASSERT_EXPIRED (error);
   ASSERT (!res);
   mongoc_client_encryption_datakey_opts_destroy (dkopts);

   /* AWS - invalid hostname. */
   memset (&error, 0, sizeof (bson_error_t));
   dkopts = mongoc_client_encryption_datakey_opts_new ();
   mongoc_client_encryption_datakey_opts_set_masterkey (dkopts,
                                                        tmp_bson ("{ 'region': 'us-east-1', 'key': "
                                                                  "'arn:aws:kms:us-east-1:579766882180:key/"
                                                                  "89fcc2c4-08b0-4bd9-9f25-e30687b580d0', 'endpoint': "
                                                                  "'127.0.0.1:9001' }"));
   res = mongoc_client_encryption_create_datakey (client_encryption_invalid_hostname, "aws", dkopts, &keyid, &error);
   ASSERT_INVALID_HOSTNAME (error);
   ASSERT (!res);
   mongoc_client_encryption_datakey_opts_destroy (dkopts);

   /* Case 2: Azure - no client cert. */
   memset (&error, 0, sizeof (bson_error_t));
   dkopts = mongoc_client_encryption_datakey_opts_new ();
   mongoc_client_encryption_datakey_opts_set_masterkey (
      dkopts, tmp_bson ("{ 'keyVaultEndpoint': 'doesnotexist.local', 'keyName': 'foo' }"));
   res = mongoc_client_encryption_create_datakey (client_encryption_no_client_cert, "azure", dkopts, &keyid, &error);
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_SOCKET, "");
   ASSERT (!res);
   mongoc_client_encryption_datakey_opts_destroy (dkopts);

   /* Azure - named with no client cert. */
   memset (&error, 0, sizeof (bson_error_t));
   dkopts = mongoc_client_encryption_datakey_opts_new ();
   mongoc_client_encryption_datakey_opts_set_masterkey (
      dkopts, tmp_bson ("{ 'keyVaultEndpoint': 'doesnotexist.local', 'keyName': 'foo' }"));
   res = mongoc_client_encryption_create_datakey (
      client_encryption_with_names, "azure:no_client_cert", dkopts, &keyid, &error);
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_SOCKET, "");
   ASSERT (!res);
   mongoc_client_encryption_datakey_opts_destroy (dkopts);

   /* Azure - with TLS. */
   memset (&error, 0, sizeof (bson_error_t));
   dkopts = mongoc_client_encryption_datakey_opts_new ();
   mongoc_client_encryption_datakey_opts_set_masterkey (
      dkopts, tmp_bson ("{ 'keyVaultEndpoint': 'doesnotexist.local', 'keyName': 'foo' }"));
   res = mongoc_client_encryption_create_datakey (client_encryption_with_tls, "azure", dkopts, &keyid, &error);
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_CLIENT_SIDE_ENCRYPTION, mongocrypt_errno, "HTTP status=404");
   ASSERT (!res);
   mongoc_client_encryption_datakey_opts_destroy (dkopts);

   /* Azure - named with TLS. */
   memset (&error, 0, sizeof (bson_error_t));
   dkopts = mongoc_client_encryption_datakey_opts_new ();
   mongoc_client_encryption_datakey_opts_set_masterkey (
      dkopts, tmp_bson ("{ 'keyVaultEndpoint': 'doesnotexist.local', 'keyName': 'foo' }"));
   res =
      mongoc_client_encryption_create_datakey (client_encryption_with_names, "azure:with_tls", dkopts, &keyid, &error);
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_CLIENT_SIDE_ENCRYPTION, mongocrypt_errno, "HTTP status=404");
   ASSERT (!res);
   mongoc_client_encryption_datakey_opts_destroy (dkopts);

   /* Azure - expired. */
   memset (&error, 0, sizeof (bson_error_t));
   dkopts = mongoc_client_encryption_datakey_opts_new ();
   mongoc_client_encryption_datakey_opts_set_masterkey (
      dkopts, tmp_bson ("{ 'keyVaultEndpoint': 'doesnotexist.local', 'keyName': 'foo' }"));
   res = mongoc_client_encryption_create_datakey (client_encryption_expired, "azure", dkopts, &keyid, &error);
   ASSERT_EXPIRED (error);
   ASSERT (!res);
   mongoc_client_encryption_datakey_opts_destroy (dkopts);

   /* Azure - invalid hostname. */
   memset (&error, 0, sizeof (bson_error_t));
   dkopts = mongoc_client_encryption_datakey_opts_new ();
   mongoc_client_encryption_datakey_opts_set_masterkey (
      dkopts, tmp_bson ("{ 'keyVaultEndpoint': 'doesnotexist.local', 'keyName': 'foo' }"));
   res = mongoc_client_encryption_create_datakey (client_encryption_invalid_hostname, "azure", dkopts, &keyid, &error);
   ASSERT_INVALID_HOSTNAME (error);
   ASSERT (!res);
   mongoc_client_encryption_datakey_opts_destroy (dkopts);

   /* Case 3: GCP - no client cert. */
   memset (&error, 0, sizeof (bson_error_t));
   dkopts = mongoc_client_encryption_datakey_opts_new ();
   mongoc_client_encryption_datakey_opts_set_masterkey (
      dkopts,
      tmp_bson ("{ 'projectId': 'pid', 'location': 'l', 'keyRing': 'kr', "
                "'keyName': 'kn' }"));
   res = mongoc_client_encryption_create_datakey (client_encryption_no_client_cert, "gcp", dkopts, &keyid, &error);
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_SOCKET, "");
   ASSERT (!res);
   mongoc_client_encryption_datakey_opts_destroy (dkopts);

   /* GCP - named with no client cert. */
   memset (&error, 0, sizeof (bson_error_t));
   dkopts = mongoc_client_encryption_datakey_opts_new ();
   mongoc_client_encryption_datakey_opts_set_masterkey (
      dkopts,
      tmp_bson ("{ 'projectId': 'pid', 'location': 'l', 'keyRing': 'kr', "
                "'keyName': 'kn' }"));
   res = mongoc_client_encryption_create_datakey (
      client_encryption_with_names, "gcp:no_client_cert", dkopts, &keyid, &error);
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_SOCKET, "");
   ASSERT (!res);
   mongoc_client_encryption_datakey_opts_destroy (dkopts);

   /* GCP - with TLS. */
   memset (&error, 0, sizeof (bson_error_t));
   dkopts = mongoc_client_encryption_datakey_opts_new ();
   mongoc_client_encryption_datakey_opts_set_masterkey (
      dkopts,
      tmp_bson ("{ 'projectId': 'pid', 'location': 'l', 'keyRing': 'kr', "
                "'keyName': 'kn' }"));
   res = mongoc_client_encryption_create_datakey (client_encryption_with_tls, "gcp", dkopts, &keyid, &error);
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_CLIENT_SIDE_ENCRYPTION, mongocrypt_errno, "HTTP status=404");
   ASSERT (!res);
   mongoc_client_encryption_datakey_opts_destroy (dkopts);

   /* GCP - named with TLS. */
   memset (&error, 0, sizeof (bson_error_t));
   dkopts = mongoc_client_encryption_datakey_opts_new ();
   mongoc_client_encryption_datakey_opts_set_masterkey (
      dkopts,
      tmp_bson ("{ 'projectId': 'pid', 'location': 'l', 'keyRing': 'kr', "
                "'keyName': 'kn' }"));
   res = mongoc_client_encryption_create_datakey (client_encryption_with_names, "gcp:with_tls", dkopts, &keyid, &error);
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_CLIENT_SIDE_ENCRYPTION, mongocrypt_errno, "HTTP status=404");
   ASSERT (!res);
   mongoc_client_encryption_datakey_opts_destroy (dkopts);

   /* GCP - expired. */
   memset (&error, 0, sizeof (bson_error_t));
   dkopts = mongoc_client_encryption_datakey_opts_new ();
   mongoc_client_encryption_datakey_opts_set_masterkey (
      dkopts,
      tmp_bson ("{ 'projectId': 'pid', 'location': 'l', 'keyRing': 'kr', "
                "'keyName': 'kn' }"));
   res = mongoc_client_encryption_create_datakey (client_encryption_expired, "gcp", dkopts, &keyid, &error);
   ASSERT_EXPIRED (error);
   ASSERT (!res);
   mongoc_client_encryption_datakey_opts_destroy (dkopts);

   /* GCP - invalid hostname. */
   memset (&error, 0, sizeof (bson_error_t));
   dkopts = mongoc_client_encryption_datakey_opts_new ();
   mongoc_client_encryption_datakey_opts_set_masterkey (
      dkopts,
      tmp_bson ("{ 'projectId': 'pid', 'location': 'l', 'keyRing': 'kr', "
                "'keyName': 'kn' }"));
   res = mongoc_client_encryption_create_datakey (client_encryption_invalid_hostname, "gcp", dkopts, &keyid, &error);
   ASSERT_INVALID_HOSTNAME (error);
   ASSERT (!res);
   mongoc_client_encryption_datakey_opts_destroy (dkopts);

   /* Case 4: KMIP - no client cert. */
   memset (&error, 0, sizeof (bson_error_t));
   dkopts = mongoc_client_encryption_datakey_opts_new ();
   mongoc_client_encryption_datakey_opts_set_masterkey (dkopts, tmp_bson ("{}"));
   res = mongoc_client_encryption_create_datakey (client_encryption_no_client_cert, "kmip", dkopts, &keyid, &error);
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_SOCKET, "");
   ASSERT (!res);
   mongoc_client_encryption_datakey_opts_destroy (dkopts);

   /* KMIP - named with no client cert. */
   memset (&error, 0, sizeof (bson_error_t));
   dkopts = mongoc_client_encryption_datakey_opts_new ();
   mongoc_client_encryption_datakey_opts_set_masterkey (dkopts, tmp_bson ("{}"));
   res = mongoc_client_encryption_create_datakey (
      client_encryption_with_names, "kmip:no_client_cert", dkopts, &keyid, &error);
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_SOCKET, "");
   ASSERT (!res);
   mongoc_client_encryption_datakey_opts_destroy (dkopts);

   /* KMIP - with TLS. */
   memset (&error, 0, sizeof (bson_error_t));
   dkopts = mongoc_client_encryption_datakey_opts_new ();
   mongoc_client_encryption_datakey_opts_set_masterkey (dkopts, tmp_bson ("{}"));
   res = mongoc_client_encryption_create_datakey (client_encryption_with_tls, "kmip", dkopts, &keyid, &error);
   ASSERT_OR_PRINT (res, error);
   bson_value_destroy (&keyid);
   mongoc_client_encryption_datakey_opts_destroy (dkopts);

   /* KMIP - named with TLS. */
   memset (&error, 0, sizeof (bson_error_t));
   dkopts = mongoc_client_encryption_datakey_opts_new ();
   mongoc_client_encryption_datakey_opts_set_masterkey (dkopts, tmp_bson ("{}"));
   res =
      mongoc_client_encryption_create_datakey (client_encryption_with_names, "kmip:with_tls", dkopts, &keyid, &error);
   ASSERT_OR_PRINT (res, error);
   bson_value_destroy (&keyid);
   mongoc_client_encryption_datakey_opts_destroy (dkopts);

   /* KMIP - expired. */
   memset (&error, 0, sizeof (bson_error_t));
   dkopts = mongoc_client_encryption_datakey_opts_new ();
   mongoc_client_encryption_datakey_opts_set_masterkey (dkopts, tmp_bson ("{}"));
   res = mongoc_client_encryption_create_datakey (client_encryption_expired, "kmip", dkopts, &keyid, &error);
   ASSERT_EXPIRED (error);
   ASSERT (!res);
   mongoc_client_encryption_datakey_opts_destroy (dkopts);

   /* KMIP - invalid hostname. */
   memset (&error, 0, sizeof (bson_error_t));
   dkopts = mongoc_client_encryption_datakey_opts_new ();
   mongoc_client_encryption_datakey_opts_set_masterkey (dkopts, tmp_bson ("{}"));
   res = mongoc_client_encryption_create_datakey (client_encryption_invalid_hostname, "kmip", dkopts, &keyid, &error);
   ASSERT_INVALID_HOSTNAME (error);
   ASSERT (!res);
   mongoc_client_encryption_datakey_opts_destroy (dkopts);

   mongoc_client_encryption_destroy (client_encryption_with_names);
   mongoc_client_encryption_destroy (client_encryption_invalid_hostname);
   mongoc_client_encryption_destroy (client_encryption_expired);
   mongoc_client_encryption_destroy (client_encryption_with_tls);
   mongoc_client_encryption_destroy (client_encryption_no_client_cert);
   mongoc_client_destroy (keyvault_client);
}

static void
test_kms_tls_options_extra_rejected (void *unused)
{
   mongoc_client_encryption_t *ce;
   mongoc_client_encryption_opts_t *ce_opts;
   mongoc_client_t *keyvault_client;
   bson_error_t error;
   bson_t *kms_providers = tmp_bson ("{'aws': {'accessKeyId': 'foo', 'secretAccessKey': 'bar'}}");

   BSON_UNUSED (unused);

   keyvault_client = test_framework_new_default_client ();

   /* Test that the "local" KMS provider is rejected. */
   ce_opts = mongoc_client_encryption_opts_new ();
   mongoc_client_encryption_opts_set_keyvault_namespace (ce_opts, "keyvault", "datakeys");
   mongoc_client_encryption_opts_set_keyvault_client (ce_opts, keyvault_client);
   mongoc_client_encryption_opts_set_kms_providers (ce_opts, kms_providers);
   mongoc_client_encryption_opts_set_tls_opts (ce_opts, tmp_bson ("{'local': {'tlsCaFile': 'ca.pem'}}"));
   ce = mongoc_client_encryption_new (ce_opts, &error);
   ASSERT_ERROR_CONTAINS (error,
                          MONGOC_ERROR_CLIENT_SIDE_ENCRYPTION,
                          MONGOC_ERROR_CLIENT_INVALID_ENCRYPTION_ARG,
                          "Cannot configure TLS options for KMS provider: local");
   ASSERT (NULL == ce);
   mongoc_client_encryption_opts_destroy (ce_opts);

   /* Test that insecure TLS options are rejected. */
   memset (&error, 0, sizeof (bson_error_t));
   ce_opts = mongoc_client_encryption_opts_new ();
   mongoc_client_encryption_opts_set_keyvault_namespace (ce_opts, "keyvault", "datakeys");
   mongoc_client_encryption_opts_set_keyvault_client (ce_opts, keyvault_client);
   mongoc_client_encryption_opts_set_kms_providers (ce_opts, kms_providers);
   mongoc_client_encryption_opts_set_tls_opts (ce_opts, tmp_bson ("{'aws': {'tlsInsecure': true}}"));
   ce = mongoc_client_encryption_new (ce_opts, &error);
   ASSERT_ERROR_CONTAINS (error,
                          MONGOC_ERROR_CLIENT_SIDE_ENCRYPTION,
                          MONGOC_ERROR_CLIENT_INVALID_ENCRYPTION_ARG,
                          "Error setting TLS option tlsInsecure for aws. "
                          "Insecure TLS options prohibited");
   ASSERT (NULL == ce);
   mongoc_client_encryption_opts_destroy (ce_opts);

   /* Test that extra TLS options are rejected. */
   memset (&error, 0, sizeof (bson_error_t));
   ce_opts = mongoc_client_encryption_opts_new ();
   mongoc_client_encryption_opts_set_keyvault_namespace (ce_opts, "keyvault", "datakeys");
   mongoc_client_encryption_opts_set_keyvault_client (ce_opts, keyvault_client);
   mongoc_client_encryption_opts_set_kms_providers (ce_opts, kms_providers);
   mongoc_client_encryption_opts_set_tls_opts (ce_opts, tmp_bson ("{'aws': {'extra': true}}"));
   ce = mongoc_client_encryption_new (ce_opts, &error);
   ASSERT_ERROR_CONTAINS (error,
                          MONGOC_ERROR_CLIENT_SIDE_ENCRYPTION,
                          MONGOC_ERROR_CLIENT_INVALID_ENCRYPTION_ARG,
                          "Error setting TLS option extra for aws. Insecure "
                          "TLS options prohibited");
   ASSERT (NULL == ce);
   mongoc_client_encryption_opts_destroy (ce_opts);

   /* Test that TLS options for duplicate providers are rejected. */
   memset (&error, 0, sizeof (bson_error_t));
   ce_opts = mongoc_client_encryption_opts_new ();
   mongoc_client_encryption_opts_set_keyvault_namespace (ce_opts, "keyvault", "datakeys");
   mongoc_client_encryption_opts_set_keyvault_client (ce_opts, keyvault_client);
   mongoc_client_encryption_opts_set_kms_providers (ce_opts, kms_providers);
   mongoc_client_encryption_opts_set_tls_opts (
      ce_opts, tmp_bson ("{'aws': {'tlsCAFile': 'foo.pem'}, 'aws': {'tlsCAFile': 'foo.pem'}}"));
   ce = mongoc_client_encryption_new (ce_opts, &error);
   ASSERT_ERROR_CONTAINS (error,
                          MONGOC_ERROR_CLIENT_SIDE_ENCRYPTION,
                          MONGOC_ERROR_CLIENT_INVALID_ENCRYPTION_ARG,
                          "Error parsing duplicate TLS options for aws");
   ASSERT (NULL == ce);
   mongoc_client_encryption_opts_destroy (ce_opts);

   /* Test that tlsDisableOCSPEndpointCheck may be set. */
   memset (&error, 0, sizeof (bson_error_t));
   ce_opts = mongoc_client_encryption_opts_new ();
   mongoc_client_encryption_opts_set_keyvault_namespace (ce_opts, "keyvault", "datakeys");
   mongoc_client_encryption_opts_set_keyvault_client (ce_opts, keyvault_client);
   mongoc_client_encryption_opts_set_kms_providers (ce_opts, kms_providers);
   mongoc_client_encryption_opts_set_tls_opts (
      ce_opts, tmp_bson ("{'aws': {'%s': true}}", MONGOC_URI_TLSDISABLEOCSPENDPOINTCHECK));
   ce = mongoc_client_encryption_new (ce_opts, &error);
   ASSERT_OR_PRINT (ce, error);
   mongoc_client_encryption_destroy (ce);
   mongoc_client_encryption_opts_destroy (ce_opts);

   mongoc_client_destroy (keyvault_client);
}

/* ee_fixture is a fixture for the Explicit Encryption prose test. */
typedef struct {
   bson_value_t key1ID;
   mongoc_client_t *keyVaultClient;
   mongoc_client_encryption_t *clientEncryption;
   mongoc_client_t *encryptedClient;
   mongoc_collection_t *encryptedColl;
} ee_fixture;

static ee_fixture *
explicit_encryption_setup (void)
{
   ee_fixture *eef = (ee_fixture *) bson_malloc0 (sizeof (ee_fixture));
   bson_t *encryptedFields =
      get_bson_from_json_file ("./src/libmongoc/tests/client_side_encryption_prose/explicit_encryption/"
                               "encryptedFields.json");
   bson_t *key1Document =
      get_bson_from_json_file ("./src/libmongoc/tests/client_side_encryption_prose/explicit_encryption/"
                               "key1-document.json");
   mongoc_client_t *setupClient = test_framework_new_default_client ();


   /* Read the ``"_id"`` field of ``key1Document`` as ``key1ID``. */
   {
      bson_iter_t iter;
      const bson_value_t *value;

      ASSERT (bson_iter_init_find (&iter, key1Document, "_id"));
      value = bson_iter_value (&iter);
      bson_value_copy (value, &eef->key1ID);
   }

   /* Drop and create the collection ``db.explicit_encryption`` using
    * ``encryptedFields`` as an option. */
   {
      mongoc_database_t *db = mongoc_client_get_database (setupClient, "db");
      mongoc_collection_t *coll = mongoc_database_get_collection (db, "explicit_encryption");
      bson_error_t error;
      bson_t *opts;

      opts = BCON_NEW ("encryptedFields", BCON_DOCUMENT (encryptedFields));

      if (!mongoc_collection_drop_with_opts (coll, opts, &error)) {
         if (error.code != MONGOC_SERVER_ERR_NS_NOT_FOUND) {
            test_error ("unexpected error in drop: %s", error.message);
         }
      }
      mongoc_collection_destroy (coll);

      coll = mongoc_database_create_collection (db, "explicit_encryption", opts, &error);
      ASSERT_OR_PRINT (coll, error);

      mongoc_collection_destroy (coll);
      bson_destroy (opts);
      mongoc_database_destroy (db);
   }

   /* Drop and create the collection ``keyvault.datakeys``. */
   {
      mongoc_database_t *db = mongoc_client_get_database (setupClient, "keyvault");
      mongoc_collection_t *coll = mongoc_database_get_collection (db, "datakeys");
      bson_error_t error;
      bson_t iopts = BSON_INITIALIZER;
      mongoc_write_concern_t *wc;

      if (!mongoc_collection_drop (coll, &error)) {
         if (error.code != MONGOC_SERVER_ERR_NS_NOT_FOUND) {
            test_error ("unexpected error in drop: %s", error.message);
         }
      }
      mongoc_collection_destroy (coll);

      coll = mongoc_database_create_collection (db, "datakeys", NULL /* opts */, &error);
      ASSERT_OR_PRINT (coll, error);

      /* Insert keyDocument1 with write concern majority */
      wc = mongoc_write_concern_new ();
      mongoc_write_concern_set_w (wc, MONGOC_WRITE_CONCERN_W_MAJORITY);
      ASSERT (mongoc_write_concern_append (wc, &iopts));
      ASSERT_OR_PRINT (mongoc_collection_insert_one (coll, key1Document, &iopts, NULL /* reply */, &error), error);

      mongoc_write_concern_destroy (wc);
      bson_destroy (&iopts);
      mongoc_collection_destroy (coll);
      mongoc_database_destroy (db);
   }

   eef->keyVaultClient = test_framework_new_default_client ();

   /* Create a ClientEncryption object named ``clientEncryption`` */
   {
      mongoc_client_encryption_opts_t *ceOpts = mongoc_client_encryption_opts_new ();
      bson_t *kms_providers = _make_local_kms_provider (NULL);
      bson_error_t error;

      mongoc_client_encryption_opts_set_keyvault_client (ceOpts, eef->keyVaultClient);
      mongoc_client_encryption_opts_set_keyvault_namespace (ceOpts, "keyvault", "datakeys");
      mongoc_client_encryption_opts_set_kms_providers (ceOpts, kms_providers);

      eef->clientEncryption = mongoc_client_encryption_new (ceOpts, &error);
      ASSERT_OR_PRINT (eef->clientEncryption, error);

      bson_destroy (kms_providers);
      mongoc_client_encryption_opts_destroy (ceOpts);
   }

   /* Create a MongoClient named ``encryptedClient``. */
   {
      mongoc_auto_encryption_opts_t *aeOpts = mongoc_auto_encryption_opts_new ();
      bson_t *kms_providers = _make_local_kms_provider (NULL);
      bson_error_t error;

      mongoc_auto_encryption_opts_set_keyvault_namespace (aeOpts, "keyvault", "datakeys");
      mongoc_auto_encryption_opts_set_kms_providers (aeOpts, kms_providers);
      mongoc_auto_encryption_opts_set_bypass_query_analysis (aeOpts, true);
      eef->encryptedClient = test_framework_new_default_client ();
      ASSERT_OR_PRINT (mongoc_client_enable_auto_encryption (eef->encryptedClient, aeOpts, &error), error);

      bson_destroy (kms_providers);
      mongoc_auto_encryption_opts_destroy (aeOpts);
      eef->encryptedColl = mongoc_client_get_collection (eef->encryptedClient, "db", "explicit_encryption");
   }

   mongoc_client_destroy (setupClient);
   bson_destroy (key1Document);
   bson_destroy (encryptedFields);
   return eef;
}

static void
explicit_encryption_destroy (ee_fixture *eef)
{
   if (!eef) {
      return;
   }

   mongoc_collection_destroy (eef->encryptedColl);
   mongoc_client_destroy (eef->encryptedClient);
   mongoc_client_encryption_destroy (eef->clientEncryption);
   mongoc_client_destroy (eef->keyVaultClient);
   bson_value_destroy (&eef->key1ID);
   bson_free (eef);
}

/* ree_fixture is a fixture for the Range Explicit Encryption prose test. */
typedef struct {
   bson_value_t key1ID;
   mongoc_client_t *keyVaultClient;
   mongoc_client_encryption_t *clientEncryption;
   mongoc_client_t *encryptedClient;
   mongoc_collection_t *encryptedColl;
   // typeStr is DoublePrecision, DoubleNoPrecision, Date, Int, or Long
   const char *typeStr;
   char *fieldName;
   bson_value_t zero;
   bson_value_t six;
   bson_value_t thirty;
   bson_value_t twoHundred;
   bson_value_t twoHundredOne;
   mongoc_client_encryption_encrypt_range_opts_t *ro;
} ree_fixture;

static ree_fixture *
range_explicit_encryption_setup (const char *typeStr)
{
   ree_fixture *reef = (ree_fixture *) bson_malloc0 (sizeof (ree_fixture));
   reef->typeStr = typeStr;
   reef->fieldName = bson_strdup_printf ("encrypted%s", typeStr);
   char *filepath = bson_strdup_printf ("./src/libmongoc/tests/client_side_encryption_prose/"
                                        "explicit_encryption/range-encryptedFields-%s.json",
                                        typeStr);
   bson_t *encryptedFields = get_bson_from_json_file (filepath);
   ASSERT (encryptedFields);
   bson_free (filepath);
   bson_t *key1Document =
      get_bson_from_json_file ("./src/libmongoc/tests/client_side_encryption_prose/explicit_encryption/"
                               "key1-document.json");
   ASSERT (key1Document);
   mongoc_client_t *setupClient = test_framework_new_default_client ();


   /* Read the ``"_id"`` field of ``key1Document`` as ``key1ID``. */
   {
      bson_iter_t iter;
      const bson_value_t *value;

      ASSERT (bson_iter_init_find (&iter, key1Document, "_id"));
      value = bson_iter_value (&iter);
      bson_value_copy (value, &reef->key1ID);
   }

   /* Drop and create the collection ``db.explicit_encryption`` using
    * ``encryptedFields`` as an option. */
   {
      mongoc_database_t *db = mongoc_client_get_database (setupClient, "db");
      mongoc_collection_t *coll = mongoc_database_get_collection (db, "explicit_encryption");
      bson_error_t error;
      bson_t *opts;

      opts = BCON_NEW ("encryptedFields", BCON_DOCUMENT (encryptedFields));

      if (!mongoc_collection_drop_with_opts (coll, opts, &error)) {
         if (error.code != MONGOC_SERVER_ERR_NS_NOT_FOUND) {
            test_error ("unexpected error in drop: %s", error.message);
         }
      }
      mongoc_collection_destroy (coll);

      coll = mongoc_database_create_collection (db, "explicit_encryption", opts, &error);
      ASSERT_OR_PRINT (coll, error);

      mongoc_collection_destroy (coll);
      bson_destroy (opts);
      mongoc_database_destroy (db);
   }

   /* Drop and create the collection ``keyvault.datakeys``. */
   {
      mongoc_database_t *db = mongoc_client_get_database (setupClient, "keyvault");
      mongoc_collection_t *coll = mongoc_database_get_collection (db, "datakeys");
      bson_error_t error;
      bson_t iopts = BSON_INITIALIZER;
      mongoc_write_concern_t *wc;

      if (!mongoc_collection_drop (coll, &error)) {
         if (error.code != MONGOC_SERVER_ERR_NS_NOT_FOUND) {
            test_error ("unexpected error in drop: %s", error.message);
         }
      }
      mongoc_collection_destroy (coll);

      coll = mongoc_database_create_collection (db, "datakeys", NULL /* opts */, &error);
      ASSERT_OR_PRINT (coll, error);

      /* Insert keyDocument1 with write concern majority */
      wc = mongoc_write_concern_new ();
      mongoc_write_concern_set_w (wc, MONGOC_WRITE_CONCERN_W_MAJORITY);
      ASSERT (mongoc_write_concern_append (wc, &iopts));
      ASSERT_OR_PRINT (mongoc_collection_insert_one (coll, key1Document, &iopts, NULL /* reply */, &error), error);

      mongoc_write_concern_destroy (wc);
      bson_destroy (&iopts);
      mongoc_collection_destroy (coll);
      mongoc_database_destroy (db);
   }

   reef->keyVaultClient = test_framework_new_default_client ();

   /* Create a ClientEncryption object named ``clientEncryption`` */
   {
      mongoc_client_encryption_opts_t *ceOpts = mongoc_client_encryption_opts_new ();
      bson_t *kms_providers = _make_local_kms_provider (NULL);
      bson_error_t error;

      mongoc_client_encryption_opts_set_keyvault_client (ceOpts, reef->keyVaultClient);
      mongoc_client_encryption_opts_set_keyvault_namespace (ceOpts, "keyvault", "datakeys");
      mongoc_client_encryption_opts_set_kms_providers (ceOpts, kms_providers);

      reef->clientEncryption = mongoc_client_encryption_new (ceOpts, &error);
      ASSERT_OR_PRINT (reef->clientEncryption, error);

      bson_destroy (kms_providers);
      mongoc_client_encryption_opts_destroy (ceOpts);
   }

   /* Create a MongoClient named ``encryptedClient``. */
   {
      mongoc_auto_encryption_opts_t *aeOpts = mongoc_auto_encryption_opts_new ();
      bson_t *kms_providers = _make_local_kms_provider (NULL);
      bson_error_t error;

      mongoc_auto_encryption_opts_set_keyvault_namespace (aeOpts, "keyvault", "datakeys");
      mongoc_auto_encryption_opts_set_kms_providers (aeOpts, kms_providers);
      mongoc_auto_encryption_opts_set_bypass_query_analysis (aeOpts, true);
      reef->encryptedClient = test_framework_new_default_client ();
      ASSERT_OR_PRINT (mongoc_client_enable_auto_encryption (reef->encryptedClient, aeOpts, &error), error);

      bson_destroy (kms_providers);
      mongoc_auto_encryption_opts_destroy (aeOpts);
      reef->encryptedColl = mongoc_client_get_collection (reef->encryptedClient, "db", "explicit_encryption");
   }

   /* Create the values 0, 6, 30, 200, and 201 as BSON values. */
   {
      if (0 == strcmp ("DecimalNoPrecision", typeStr) || 0 == strcmp ("DecimalPrecision", typeStr)) {
         reef->zero.value_type = BSON_TYPE_DECIMAL128;
         ASSERT (bson_decimal128_from_string ("0", &reef->zero.value.v_decimal128));
         reef->six.value_type = BSON_TYPE_DECIMAL128;
         ASSERT (bson_decimal128_from_string ("6", &reef->six.value.v_decimal128));
         reef->thirty.value_type = BSON_TYPE_DECIMAL128;
         ASSERT (bson_decimal128_from_string ("30", &reef->thirty.value.v_decimal128));
         reef->twoHundred.value_type = BSON_TYPE_DECIMAL128;
         ASSERT (bson_decimal128_from_string ("200", &reef->twoHundred.value.v_decimal128));
         reef->twoHundredOne.value_type = BSON_TYPE_DECIMAL128;
         ASSERT (bson_decimal128_from_string ("201", &reef->twoHundredOne.value.v_decimal128));
      } else if (0 == strcmp ("DoubleNoPrecision", typeStr) || 0 == strcmp ("DoublePrecision", typeStr)) {
         reef->zero.value_type = BSON_TYPE_DOUBLE;
         reef->zero.value.v_double = 0;
         reef->six.value_type = BSON_TYPE_DOUBLE;
         reef->six.value.v_double = 6;
         reef->thirty.value_type = BSON_TYPE_DOUBLE;
         reef->thirty.value.v_double = 30;
         reef->twoHundred.value_type = BSON_TYPE_DOUBLE;
         reef->twoHundred.value.v_double = 200;
         reef->twoHundredOne.value_type = BSON_TYPE_DOUBLE;
         reef->twoHundredOne.value.v_double = 201;
      } else if (0 == strcmp ("Date", typeStr)) {
         reef->zero.value_type = BSON_TYPE_DATE_TIME;
         reef->zero.value.v_datetime = 0;
         reef->six.value_type = BSON_TYPE_DATE_TIME;
         reef->six.value.v_datetime = 6;
         reef->thirty.value_type = BSON_TYPE_DATE_TIME;
         reef->thirty.value.v_datetime = 30;
         reef->twoHundred.value_type = BSON_TYPE_DATE_TIME;
         reef->twoHundred.value.v_datetime = 200;
         reef->twoHundredOne.value_type = BSON_TYPE_DATE_TIME;
         reef->twoHundredOne.value.v_datetime = 201;
      } else if (0 == strcmp ("Int", typeStr)) {
         reef->zero.value_type = BSON_TYPE_INT32;
         reef->zero.value.v_int32 = 0;
         reef->six.value_type = BSON_TYPE_INT32;
         reef->six.value.v_int32 = 6;
         reef->thirty.value_type = BSON_TYPE_INT32;
         reef->thirty.value.v_int32 = 30;
         reef->twoHundred.value_type = BSON_TYPE_INT32;
         reef->twoHundred.value.v_int32 = 200;
         reef->twoHundredOne.value_type = BSON_TYPE_INT32;
         reef->twoHundredOne.value.v_int32 = 201;
      } else if (0 == strcmp ("Long", typeStr)) {
         reef->zero.value_type = BSON_TYPE_INT64;
         reef->zero.value.v_int64 = 0;
         reef->six.value_type = BSON_TYPE_INT64;
         reef->six.value.v_int64 = 6;
         reef->thirty.value_type = BSON_TYPE_INT64;
         reef->thirty.value.v_int64 = 30;
         reef->twoHundred.value_type = BSON_TYPE_INT64;
         reef->twoHundred.value.v_int64 = 200;
         reef->twoHundredOne.value_type = BSON_TYPE_INT64;
         reef->twoHundredOne.value.v_int64 = 201;
      } else {
         test_error ("Unexpected type string: %s\n", typeStr);
      }
   }

   /* Create the RangeOpts depending on the type. */
   {
      reef->ro = mongoc_client_encryption_encrypt_range_opts_new ();
      mongoc_client_encryption_encrypt_range_opts_set_sparsity (reef->ro, 1);
      if (0 == strcmp ("DoubleNoPrecision", typeStr) || 0 == strcmp ("DecimalNoPrecision", typeStr)) {
         // DoubleNoPrecision does not need more range options.
      } else if (0 == strcmp ("DoublePrecision", typeStr) || 0 == strcmp ("DecimalPrecision", typeStr)) {
         mongoc_client_encryption_encrypt_range_opts_set_min (reef->ro, &reef->zero);
         mongoc_client_encryption_encrypt_range_opts_set_max (reef->ro, &reef->twoHundred);
         mongoc_client_encryption_encrypt_range_opts_set_precision (reef->ro, 2);
      } else if (0 == strcmp ("Date", typeStr) || 0 == strcmp ("Int", typeStr) || 0 == strcmp ("Long", typeStr)) {
         mongoc_client_encryption_encrypt_range_opts_set_min (reef->ro, &reef->zero);
         mongoc_client_encryption_encrypt_range_opts_set_max (reef->ro, &reef->twoHundred);
      } else {
         test_error ("Unexpected type string: %s\n", typeStr);
      }
   }

   /* Encrypt and insert 0, 6, 30, and 200. */
   {
      mongoc_client_encryption_encrypt_opts_t *eo;
      bool ok;
      bson_error_t error;

      eo = mongoc_client_encryption_encrypt_opts_new ();
      mongoc_client_encryption_encrypt_opts_set_keyid (eo, &reef->key1ID);
      mongoc_client_encryption_encrypt_opts_set_algorithm (eo, MONGOC_ENCRYPT_ALGORITHM_RANGEPREVIEW);
      mongoc_client_encryption_encrypt_opts_set_contention_factor (eo, 0);
      mongoc_client_encryption_encrypt_opts_set_range_opts (eo, reef->ro);

      bson_value_t *values[4];
      values[0] = &reef->zero;
      values[1] = &reef->six;
      values[2] = &reef->thirty;
      values[3] = &reef->twoHundred;

      for (size_t i = 0; i < sizeof values / sizeof values[0]; i++) {
         bson_value_t *value = values[i];
         bson_value_t ciphertext;
         ok = mongoc_client_encryption_encrypt (reef->clientEncryption, value, eo, &ciphertext, &error);
         ASSERT_OR_PRINT (ok, error);
         bson_t *doc = bson_new ();
         BSON_APPEND_INT32 (doc, "_id", (int32_t) i);
         BSON_APPEND_VALUE (doc, reef->fieldName, &ciphertext);
         ok = mongoc_collection_insert_one (reef->encryptedColl, doc, NULL /* opts */, NULL /* reply */, &error);
         ASSERT_OR_PRINT (ok, error);
         bson_destroy (doc);
         bson_value_destroy (&ciphertext);
      }

      mongoc_client_encryption_encrypt_opts_destroy (eo);
   }

   mongoc_client_destroy (setupClient);
   bson_destroy (key1Document);
   bson_destroy (encryptedFields);
   return reef;
}

static void
range_explicit_encryption_destroy (ree_fixture *reef)
{
   if (!reef) {
      return;
   }

   bson_free (reef->fieldName);
   mongoc_client_encryption_encrypt_range_opts_destroy (reef->ro);
   mongoc_collection_destroy (reef->encryptedColl);
   mongoc_client_destroy (reef->encryptedClient);
   mongoc_client_encryption_destroy (reef->clientEncryption);
   mongoc_client_destroy (reef->keyVaultClient);
   bson_value_destroy (&reef->key1ID);
   bson_free (reef);
}

// range_explicit_encryption_assert_cursor_results asserts that the encrypted
// field in documents returned by the cursor have match a list of values. The
// variadic args are a list of const bson_value_t *. The variadic args must be
// NULL terminated.
static void
range_explicit_encryption_assert_cursor_results (ree_fixture *reef, mongoc_cursor_t *cursor, ...)
{
   const bson_t *got;
   va_list args;
   bson_error_t error;

   va_start (args, cursor);
   while (true) {
      const bson_value_t *expect = va_arg (args, const bson_value_t *);
      if (!expect) {
         break;
      }
      if (!mongoc_cursor_next (cursor, &got)) {
         if (mongoc_cursor_error (cursor, &error)) {
            test_error ("Got unexpected error in mongoc_cursor_next: %s", error.message);
         }
         test_error ("Expected document with value: %s but got end of cursor", bson_value_to_str (expect));
      }
      bson_iter_t goti;
      const bson_value_t *gotv;
      if (!bson_iter_init_find (&goti, got, reef->fieldName)) {
         test_error (
            "Expected to find field %s, but got %s", reef->fieldName, bson_as_canonical_extended_json (got, NULL));
      }
      gotv = bson_iter_value (&goti);
      ASSERT_BSONVALUE_EQ (expect, gotv);
   }
   va_end (args);
   if (mongoc_cursor_next (cursor, &got)) {
      test_error ("Expected end of cursor, but got extra document: %s", bson_as_canonical_extended_json (got, NULL));
   }
}

static void
test_range_explicit_encryption_case1 (void *ctx)
{
   // Case 1: can decrypt a payload
   const char *typeStr = (const char *) ctx;
   mongoc_client_encryption_encrypt_opts_t *eo;
   ree_fixture *reef = range_explicit_encryption_setup (typeStr);
   bool ok;
   bson_error_t error;
   bson_value_t insertPayload;
   bson_value_t decrypted;

   eo = mongoc_client_encryption_encrypt_opts_new ();
   mongoc_client_encryption_encrypt_opts_set_keyid (eo, &reef->key1ID);
   mongoc_client_encryption_encrypt_opts_set_algorithm (eo, MONGOC_ENCRYPT_ALGORITHM_RANGEPREVIEW);
   mongoc_client_encryption_encrypt_opts_set_contention_factor (eo, 0);
   mongoc_client_encryption_encrypt_opts_set_range_opts (eo, reef->ro);

   /* Use clientEncryption.encrypt() to encrypt the value 6. */
   ok = mongoc_client_encryption_encrypt (reef->clientEncryption, &reef->six, eo, &insertPayload, &error);
   ASSERT_OR_PRINT (ok, error);

   /* Use clientEncryption to decrypt insertPayload. Assert the returned
      value equals 6. */
   ok = mongoc_client_encryption_decrypt (reef->clientEncryption, &insertPayload, &decrypted, &error);
   ASSERT_OR_PRINT (ok, error);
   ASSERT_BSONVALUE_EQ (&decrypted, &reef->six);

   bson_value_destroy (&insertPayload);
   mongoc_client_encryption_encrypt_opts_destroy (eo);
   range_explicit_encryption_destroy (reef);
}

static void
test_range_explicit_encryption_case2 (void *ctx)
{
   // Case 2: can find encrypted range and return the maximum
   const char *typeStr = (const char *) ctx;
   ree_fixture *reef = range_explicit_encryption_setup (typeStr);
   bool ok;
   bson_error_t error;

   // Encrypt.
   bson_t findPayload;
   {
      mongoc_client_encryption_encrypt_opts_t *eo = mongoc_client_encryption_encrypt_opts_new ();
      mongoc_client_encryption_encrypt_opts_set_keyid (eo, &reef->key1ID);
      mongoc_client_encryption_encrypt_opts_set_algorithm (eo, MONGOC_ENCRYPT_ALGORITHM_RANGEPREVIEW);
      mongoc_client_encryption_encrypt_opts_set_query_type (eo, MONGOC_ENCRYPT_QUERY_TYPE_RANGEPREVIEW);
      mongoc_client_encryption_encrypt_opts_set_contention_factor (eo, 0);
      mongoc_client_encryption_encrypt_opts_set_range_opts (eo, reef->ro);

      bsonBuildDecl (expr,
                     kv ("$and",
                         array (                                             //
                            doc (kv (reef->fieldName,                        //
                                     doc (kv ("$gte", value (reef->six))))), //
                            doc (kv (reef->fieldName, doc (kv ("$lte", value (reef->twoHundred))))))));

      ok = mongoc_client_encryption_encrypt_expression (reef->clientEncryption, &expr, eo, &findPayload, &error);
      ASSERT_OR_PRINT (ok, error);
      bson_destroy (&expr);
      mongoc_client_encryption_encrypt_opts_destroy (eo);
   }

   // Query and check results.
   {
      bson_t *opts = tmp_bson ("{'sort': { '_id': 1 }}");
      mongoc_cursor_t *cursor =
         mongoc_collection_find_with_opts (reef->encryptedColl, &findPayload, opts, NULL /* read_prefs */);
      range_explicit_encryption_assert_cursor_results (
         reef, cursor, &reef->six, &reef->thirty, &reef->twoHundred, NULL);
      mongoc_cursor_destroy (cursor);
   }

   bson_destroy (&findPayload);
   range_explicit_encryption_destroy (reef);
}

static void
test_range_explicit_encryption_case3 (void *ctx)
{
   // Case 3: can find encrypted range and return the minimum
   const char *typeStr = (const char *) ctx;
   ree_fixture *reef = range_explicit_encryption_setup (typeStr);
   bool ok;
   bson_error_t error;

   // Encrypt.
   bson_t findPayload;
   {
      mongoc_client_encryption_encrypt_opts_t *eo = mongoc_client_encryption_encrypt_opts_new ();
      mongoc_client_encryption_encrypt_opts_set_keyid (eo, &reef->key1ID);
      mongoc_client_encryption_encrypt_opts_set_algorithm (eo, MONGOC_ENCRYPT_ALGORITHM_RANGEPREVIEW);
      mongoc_client_encryption_encrypt_opts_set_query_type (eo, MONGOC_ENCRYPT_QUERY_TYPE_RANGEPREVIEW);
      mongoc_client_encryption_encrypt_opts_set_contention_factor (eo, 0);
      mongoc_client_encryption_encrypt_opts_set_range_opts (eo, reef->ro);

      bsonBuildDecl (expr,
                     kv ("$and",
                         array (                                              //
                            doc (kv (reef->fieldName,                         //
                                     doc (kv ("$gte", value (reef->zero))))), //
                            doc (kv (reef->fieldName, doc (kv ("$lte", value (reef->six))))))));

      ok = mongoc_client_encryption_encrypt_expression (reef->clientEncryption, &expr, eo, &findPayload, &error);
      ASSERT_OR_PRINT (ok, error);
      bson_destroy (&expr);
      mongoc_client_encryption_encrypt_opts_destroy (eo);
   }

   // Query and check results.
   {
      bson_t *opts = tmp_bson ("{'sort': { '_id': 1 }}");
      mongoc_cursor_t *cursor =
         mongoc_collection_find_with_opts (reef->encryptedColl, &findPayload, opts, NULL /* read_prefs */);
      range_explicit_encryption_assert_cursor_results (reef, cursor, &reef->zero, &reef->six, NULL);
      mongoc_cursor_destroy (cursor);
   }

   bson_destroy (&findPayload);
   range_explicit_encryption_destroy (reef);
}

static void
test_range_explicit_encryption_case4 (void *ctx)
{
   // Case 4: can find encrypted range with an open range query
   const char *typeStr = (const char *) ctx;
   ree_fixture *reef = range_explicit_encryption_setup (typeStr);
   bool ok;
   bson_error_t error;

   // Encrypt.
   bson_t findPayload;
   {
      mongoc_client_encryption_encrypt_opts_t *eo = mongoc_client_encryption_encrypt_opts_new ();
      mongoc_client_encryption_encrypt_opts_set_keyid (eo, &reef->key1ID);
      mongoc_client_encryption_encrypt_opts_set_algorithm (eo, MONGOC_ENCRYPT_ALGORITHM_RANGEPREVIEW);
      mongoc_client_encryption_encrypt_opts_set_query_type (eo, MONGOC_ENCRYPT_QUERY_TYPE_RANGEPREVIEW);
      mongoc_client_encryption_encrypt_opts_set_contention_factor (eo, 0);
      mongoc_client_encryption_encrypt_opts_set_range_opts (eo, reef->ro);

      bsonBuildDecl (expr,
                     kv ("$and",
                         array (                                              //
                            doc (kv (reef->fieldName,                         //
                                     doc (kv ("$gt", value (reef->thirty))))) //
                            )));

      ok = mongoc_client_encryption_encrypt_expression (reef->clientEncryption, &expr, eo, &findPayload, &error);
      ASSERT_OR_PRINT (ok, error);
      bson_destroy (&expr);
      mongoc_client_encryption_encrypt_opts_destroy (eo);
   }

   // Query and check results.
   {
      bson_t *opts = tmp_bson ("{'sort': { '_id': 1 }}");
      mongoc_cursor_t *cursor =
         mongoc_collection_find_with_opts (reef->encryptedColl, &findPayload, opts, NULL /* read_prefs */);
      range_explicit_encryption_assert_cursor_results (reef, cursor, &reef->twoHundred, NULL);
      mongoc_cursor_destroy (cursor);
   }

   bson_destroy (&findPayload);
   range_explicit_encryption_destroy (reef);
}

static void
test_range_explicit_encryption_case5 (void *ctx)
{
   // Case 5: can run an aggregation expression inside $expr
   const char *typeStr = (const char *) ctx;
   ree_fixture *reef = range_explicit_encryption_setup (typeStr);
   bool ok;
   bson_error_t error;

   // Encrypt.
   bson_t findPayload;
   {
      mongoc_client_encryption_encrypt_opts_t *eo = mongoc_client_encryption_encrypt_opts_new ();
      mongoc_client_encryption_encrypt_opts_set_keyid (eo, &reef->key1ID);
      mongoc_client_encryption_encrypt_opts_set_algorithm (eo, MONGOC_ENCRYPT_ALGORITHM_RANGEPREVIEW);
      mongoc_client_encryption_encrypt_opts_set_query_type (eo, MONGOC_ENCRYPT_QUERY_TYPE_RANGEPREVIEW);
      mongoc_client_encryption_encrypt_opts_set_contention_factor (eo, 0);
      mongoc_client_encryption_encrypt_opts_set_range_opts (eo, reef->ro);
      char *fieldPath = bson_strdup_printf ("$%s", reef->fieldName);

      // Encrypt this:
      // {'$and': [ { '$lt': [ '$encrypted<Type>', 30 ] } ] } }
      bsonBuildDecl (expr,
                     kv ("$and",
                         array (            //
                            doc (kv ("$lt", //
                                     array (cstr (fieldPath), value (reef->thirty)))))));

      ok = mongoc_client_encryption_encrypt_expression (reef->clientEncryption, &expr, eo, &findPayload, &error);
      ASSERT_OR_PRINT (ok, error);
      bson_free (fieldPath);
      bson_destroy (&expr);
      mongoc_client_encryption_encrypt_opts_destroy (eo);
   }

   // Query and check results.
   {
      bson_t *opts = tmp_bson ("{'sort': { '_id': 1 }}");
      bsonBuildDecl (filter, kv ("$expr", bson (findPayload)));
      mongoc_cursor_t *cursor =
         mongoc_collection_find_with_opts (reef->encryptedColl, &filter, opts, NULL /* read_prefs */);
      range_explicit_encryption_assert_cursor_results (reef, cursor, &reef->zero, &reef->six, NULL);
      bson_destroy (&filter);
      mongoc_cursor_destroy (cursor);
   }

   bson_destroy (&findPayload);
   range_explicit_encryption_destroy (reef);
}

static void
test_range_explicit_encryption_case6 (void *ctx)
{
   // Case 6: encrypting a document greater than the maximum errors
   const char *typeStr = (const char *) ctx;

   // This test case should be skipped if the encrypted field is
   // encryptedDoubleNoPrecision or encryptedDecimalNoPrecision.
   if (0 == strcmp (typeStr, "DoubleNoPrecision") || 0 == strcmp (typeStr, "DecimalNoPrecision")) {
      MONGOC_DEBUG ("skipping test");
      return;
   }

   ree_fixture *reef = range_explicit_encryption_setup (typeStr);
   bool ok;
   bson_error_t error;

   // Encrypt.
   bson_value_t insertPayload;
   {
      mongoc_client_encryption_encrypt_opts_t *eo = mongoc_client_encryption_encrypt_opts_new ();
      mongoc_client_encryption_encrypt_opts_set_keyid (eo, &reef->key1ID);
      mongoc_client_encryption_encrypt_opts_set_algorithm (eo, MONGOC_ENCRYPT_ALGORITHM_RANGEPREVIEW);
      mongoc_client_encryption_encrypt_opts_set_contention_factor (eo, 0);
      mongoc_client_encryption_encrypt_opts_set_range_opts (eo, reef->ro);

      ok = mongoc_client_encryption_encrypt (reef->clientEncryption, &reef->twoHundredOne, eo, &insertPayload, &error);
      ASSERT (!ok);
      mongoc_client_encryption_encrypt_opts_destroy (eo);
   }

   bson_value_destroy (&insertPayload);
   range_explicit_encryption_destroy (reef);
}

static void
test_range_explicit_encryption_case7 (void *ctx)
{
   // Case 7: encrypting a document of a different type errors
   const char *typeStr = (const char *) ctx;

   // This test case should be skipped if the encrypted field is
   // encryptedDoubleNoPrecision or encryptedDecimalNoPrecision.
   if (0 == strcmp (typeStr, "DoubleNoPrecision") || 0 == strcmp (typeStr, "DecimalNoPrecision")) {
      MONGOC_DEBUG ("skipping test");
      return;
   }

   ree_fixture *reef = range_explicit_encryption_setup (typeStr);
   bool ok;
   bson_error_t error;

   // Encrypt.
   bson_value_t insertPayload;
   {
      mongoc_client_encryption_encrypt_opts_t *eo = mongoc_client_encryption_encrypt_opts_new ();
      mongoc_client_encryption_encrypt_opts_set_keyid (eo, &reef->key1ID);
      mongoc_client_encryption_encrypt_opts_set_algorithm (eo, MONGOC_ENCRYPT_ALGORITHM_RANGEPREVIEW);
      mongoc_client_encryption_encrypt_opts_set_contention_factor (eo, 0);
      mongoc_client_encryption_encrypt_opts_set_range_opts (eo, reef->ro);

      bson_value_t wrongType;
      if (0 == strcmp ("encryptedInt", reef->fieldName)) {
         wrongType.value_type = BSON_TYPE_DOUBLE;
         wrongType.value.v_double = 6;
      } else {
         wrongType.value_type = BSON_TYPE_INT32;
         wrongType.value.v_int32 = 6;
      }

      ok = mongoc_client_encryption_encrypt (reef->clientEncryption, &wrongType, eo, &insertPayload, &error);
      ASSERT (!ok);
      mongoc_client_encryption_encrypt_opts_destroy (eo);
   }

   bson_value_destroy (&insertPayload);
   range_explicit_encryption_destroy (reef);
}

static void
test_range_explicit_encryption_case8 (void *ctx)
{
   // Case 8: setting precision errors if the type is not a double
   const char *typeStr = (const char *) ctx;

   // This test case should be skipped if the encrypted field is
   // encryptedDoublePrecision or encryptedDoubleNoPrecision or
   // encryptedDecimalPrecision or encryptedDecimalNoPrecision.
   if (0 == strcmp (typeStr, "DoubleNoPrecision") || 0 == strcmp (typeStr, "DoublePrecision") ||
       0 == strcmp (typeStr, "DecimalPrecision") || 0 == strcmp (typeStr, "DecimalNoPrecision")) {
      MONGOC_DEBUG ("skipping test");
      return;
   }

   ree_fixture *reef = range_explicit_encryption_setup (typeStr);
   bool ok;
   bson_error_t error;

   // Encrypt.
   bson_value_t insertPayload;
   {
      mongoc_client_encryption_encrypt_opts_t *eo = mongoc_client_encryption_encrypt_opts_new ();
      mongoc_client_encryption_encrypt_opts_set_keyid (eo, &reef->key1ID);
      mongoc_client_encryption_encrypt_opts_set_algorithm (eo, MONGOC_ENCRYPT_ALGORITHM_RANGEPREVIEW);
      mongoc_client_encryption_encrypt_opts_set_contention_factor (eo, 0);
      mongoc_client_encryption_encrypt_range_opts_set_precision (reef->ro, 2);
      mongoc_client_encryption_encrypt_opts_set_range_opts (eo, reef->ro);

      ok = mongoc_client_encryption_encrypt (reef->clientEncryption, &reef->six, eo, &insertPayload, &error);
      ASSERT (!ok);
      mongoc_client_encryption_encrypt_opts_destroy (eo);
   }

   bson_value_destroy (&insertPayload);
   range_explicit_encryption_destroy (reef);
}

static void
test_explicit_encryption_case1 (void *unused)
{
   /* Case 1: can insert encrypted indexed and find */
   bson_error_t error;
   bool ok;
   mongoc_client_encryption_encrypt_opts_t *eopts;
   bson_value_t plaintext = {0};
   ee_fixture *eef = explicit_encryption_setup ();

   BSON_UNUSED (unused);

   plaintext.value_type = BSON_TYPE_UTF8;
   plaintext.value.v_utf8.str = "encrypted indexed value";
   plaintext.value.v_utf8.len = (uint32_t) strlen (plaintext.value.v_utf8.str);

   /* Use ``encryptedClient`` to insert the document ``{ "encryptedIndexed":
    * <insertPayload> }``. */
   {
      bson_value_t insertPayload;
      bson_t to_insert = BSON_INITIALIZER;

      eopts = mongoc_client_encryption_encrypt_opts_new ();
      mongoc_client_encryption_encrypt_opts_set_keyid (eopts, &eef->key1ID);
      mongoc_client_encryption_encrypt_opts_set_algorithm (eopts, MONGOC_ENCRYPT_ALGORITHM_INDEXED);
      mongoc_client_encryption_encrypt_opts_set_contention_factor (eopts, 0);

      ok = mongoc_client_encryption_encrypt (eef->clientEncryption, &plaintext, eopts, &insertPayload, &error);
      ASSERT_OR_PRINT (ok, error);

      ASSERT (BSON_APPEND_VALUE (&to_insert, "encryptedIndexed", &insertPayload));

      ok = mongoc_collection_insert_one (eef->encryptedColl, &to_insert, NULL /* opts */, NULL /* reply */, &error);
      ASSERT_OR_PRINT (ok, error);

      bson_value_destroy (&insertPayload);
      bson_destroy (&to_insert);
      mongoc_client_encryption_encrypt_opts_destroy (eopts);
   }

   /* Use ``encryptedClient`` to run a "find" operation on the
    * ``db.explicit_encryption`` collection with the filter ``{
    * "encryptedIndexed": <findPayload> }``. */
   {
      bson_value_t findPayload;
      mongoc_cursor_t *cursor;
      bson_t filter = BSON_INITIALIZER;
      const bson_t *got;

      eopts = mongoc_client_encryption_encrypt_opts_new ();
      mongoc_client_encryption_encrypt_opts_set_keyid (eopts, &eef->key1ID);
      mongoc_client_encryption_encrypt_opts_set_algorithm (eopts, MONGOC_ENCRYPT_ALGORITHM_INDEXED);
      mongoc_client_encryption_encrypt_opts_set_query_type (eopts, MONGOC_ENCRYPT_QUERY_TYPE_EQUALITY);
      mongoc_client_encryption_encrypt_opts_set_contention_factor (eopts, 0);

      ok = mongoc_client_encryption_encrypt (eef->clientEncryption, &plaintext, eopts, &findPayload, &error);
      ASSERT_OR_PRINT (ok, error);

      ASSERT (BSON_APPEND_VALUE (&filter, "encryptedIndexed", &findPayload));

      cursor = mongoc_collection_find_with_opts (eef->encryptedColl, &filter, NULL /* opts */, NULL /* read_prefs */);
      ASSERT (mongoc_cursor_next (cursor, &got));
      ASSERT_OR_PRINT (!mongoc_cursor_error (cursor, &error), error);
      ASSERT_MATCH (got, "{ 'encryptedIndexed': 'encrypted indexed value' }");
      ASSERT (!mongoc_cursor_next (cursor, &got) && "expected one document to be returned, got more than one");

      bson_value_destroy (&findPayload);
      mongoc_cursor_destroy (cursor);
      mongoc_client_encryption_encrypt_opts_destroy (eopts);
      bson_destroy (&filter);
   }

   explicit_encryption_destroy (eef);
}

static void
test_explicit_encryption_case2 (void *unused)
{
   /* Case 2: can insert encrypted indexed and find with non-zero contention */
   bson_error_t error;
   bool ok;
   mongoc_client_encryption_encrypt_opts_t *eopts;
   bson_value_t plaintext = {0};
   int i = 0;
   ee_fixture *eef = explicit_encryption_setup ();

   BSON_UNUSED (unused);

   plaintext.value_type = BSON_TYPE_UTF8;
   plaintext.value.v_utf8.str = "encrypted indexed value";
   plaintext.value.v_utf8.len = (uint32_t) strlen (plaintext.value.v_utf8.str);

   /* Insert 10 documents ``{ "encryptedIndexed": <insertPayload> }`` with
    * contention factor 10. */
   for (i = 0; i < 10; i++) {
      bson_value_t insertPayload;
      bson_t to_insert = BSON_INITIALIZER;

      eopts = mongoc_client_encryption_encrypt_opts_new ();
      mongoc_client_encryption_encrypt_opts_set_keyid (eopts, &eef->key1ID);
      mongoc_client_encryption_encrypt_opts_set_algorithm (eopts, MONGOC_ENCRYPT_ALGORITHM_INDEXED);
      mongoc_client_encryption_encrypt_opts_set_contention_factor (eopts, 10);

      ok = mongoc_client_encryption_encrypt (eef->clientEncryption, &plaintext, eopts, &insertPayload, &error);
      ASSERT_OR_PRINT (ok, error);

      ASSERT (BSON_APPEND_VALUE (&to_insert, "encryptedIndexed", &insertPayload));

      ok = mongoc_collection_insert_one (eef->encryptedColl, &to_insert, NULL /* opts */, NULL /* reply */, &error);
      ASSERT_OR_PRINT (ok, error);

      bson_value_destroy (&insertPayload);
      bson_destroy (&to_insert);
      mongoc_client_encryption_encrypt_opts_destroy (eopts);
   }

   /* Find with contention factor of 0. Expect < 10 documents returned.
    */
   {
      bson_value_t findPayload;
      mongoc_cursor_t *cursor;
      bson_t filter = BSON_INITIALIZER;
      const bson_t *got;
      int got_count = 0;

      eopts = mongoc_client_encryption_encrypt_opts_new ();
      mongoc_client_encryption_encrypt_opts_set_keyid (eopts, &eef->key1ID);
      mongoc_client_encryption_encrypt_opts_set_algorithm (eopts, MONGOC_ENCRYPT_ALGORITHM_INDEXED);
      mongoc_client_encryption_encrypt_opts_set_query_type (eopts, MONGOC_ENCRYPT_QUERY_TYPE_EQUALITY);
      mongoc_client_encryption_encrypt_opts_set_contention_factor (eopts, 0);

      ok = mongoc_client_encryption_encrypt (eef->clientEncryption, &plaintext, eopts, &findPayload, &error);
      ASSERT_OR_PRINT (ok, error);

      ASSERT (BSON_APPEND_VALUE (&filter, "encryptedIndexed", &findPayload));

      cursor = mongoc_collection_find_with_opts (eef->encryptedColl, &filter, NULL /* opts */, NULL /* read_prefs */);

      while (mongoc_cursor_next (cursor, &got)) {
         got_count++;
         ASSERT_MATCH (got, "{ 'encryptedIndexed': 'encrypted indexed value' }");
      }
      ASSERT_OR_PRINT (!mongoc_cursor_error (cursor, &error), error);
      ASSERT_CMPINT (got_count, <, 10);

      bson_value_destroy (&findPayload);
      mongoc_cursor_destroy (cursor);
      mongoc_client_encryption_encrypt_opts_destroy (eopts);
      bson_destroy (&filter);
   }

   /* Find with contention factor of 10. Expect all 10 documents returned. */
   {
      bson_value_t findPayload;
      mongoc_cursor_t *cursor;
      bson_t filter = BSON_INITIALIZER;
      const bson_t *got;
      int got_count = 0;

      eopts = mongoc_client_encryption_encrypt_opts_new ();
      mongoc_client_encryption_encrypt_opts_set_keyid (eopts, &eef->key1ID);
      mongoc_client_encryption_encrypt_opts_set_algorithm (eopts, MONGOC_ENCRYPT_ALGORITHM_INDEXED);
      mongoc_client_encryption_encrypt_opts_set_query_type (eopts, MONGOC_ENCRYPT_QUERY_TYPE_EQUALITY);
      mongoc_client_encryption_encrypt_opts_set_contention_factor (eopts, 10);

      ok = mongoc_client_encryption_encrypt (eef->clientEncryption, &plaintext, eopts, &findPayload, &error);
      ASSERT_OR_PRINT (ok, error);

      ASSERT (BSON_APPEND_VALUE (&filter, "encryptedIndexed", &findPayload));

      cursor = mongoc_collection_find_with_opts (eef->encryptedColl, &filter, NULL /* opts */, NULL /* read_prefs */);

      while (mongoc_cursor_next (cursor, &got)) {
         got_count++;
         ASSERT_MATCH (got, "{ 'encryptedIndexed': 'encrypted indexed value' }");
      }
      ASSERT_OR_PRINT (!mongoc_cursor_error (cursor, &error), error);
      ASSERT_CMPINT (got_count, ==, 10);

      bson_value_destroy (&findPayload);
      mongoc_cursor_destroy (cursor);
      mongoc_client_encryption_encrypt_opts_destroy (eopts);
      bson_destroy (&filter);
   }

   explicit_encryption_destroy (eef);
}

static void
test_explicit_encryption_case3 (void *unused)
{
   /* Case 3: can insert encrypted unindexed */
   bson_error_t error;
   bool ok;
   mongoc_client_encryption_encrypt_opts_t *eopts;
   bson_value_t plaintext = {0};
   ee_fixture *eef = explicit_encryption_setup ();

   BSON_UNUSED (unused);

   plaintext.value_type = BSON_TYPE_UTF8;
   plaintext.value.v_utf8.str = "encrypted unindexed value";
   plaintext.value.v_utf8.len = (uint32_t) strlen (plaintext.value.v_utf8.str);

   /* Use ``encryptedClient`` to insert the document ``{ "_id": 1,
    * "encryptedUnindexed": <insertPayload> }``. */
   {
      bson_value_t insertPayload;
      bson_t to_insert = BSON_INITIALIZER;

      eopts = mongoc_client_encryption_encrypt_opts_new ();
      mongoc_client_encryption_encrypt_opts_set_keyid (eopts, &eef->key1ID);
      mongoc_client_encryption_encrypt_opts_set_algorithm (eopts, MONGOC_ENCRYPT_ALGORITHM_UNINDEXED);

      ok = mongoc_client_encryption_encrypt (eef->clientEncryption, &plaintext, eopts, &insertPayload, &error);
      ASSERT_OR_PRINT (ok, error);

      ASSERT (BSON_APPEND_INT32 (&to_insert, "_id", 1));
      ASSERT (BSON_APPEND_VALUE (&to_insert, "encryptedUnindexed", &insertPayload));

      ok = mongoc_collection_insert_one (eef->encryptedColl, &to_insert, NULL /* opts */, NULL /* reply */, &error);
      ASSERT_OR_PRINT (ok, error);

      bson_value_destroy (&insertPayload);
      bson_destroy (&to_insert);
      mongoc_client_encryption_encrypt_opts_destroy (eopts);
   }

   /* Use ``encryptedClient`` to run a "find" operation on the
    * ``db.explicit_encryption`` collection with the filter ``{ "_id": 1 }``. */
   {
      mongoc_cursor_t *cursor;
      bson_t filter = BSON_INITIALIZER;
      const bson_t *got;

      ASSERT (BSON_APPEND_INT32 (&filter, "_id", 1));

      cursor = mongoc_collection_find_with_opts (eef->encryptedColl, &filter, NULL /* opts */, NULL /* read_prefs */);
      ASSERT (mongoc_cursor_next (cursor, &got));
      ASSERT_OR_PRINT (!mongoc_cursor_error (cursor, &error), error);
      ASSERT_MATCH (got, "{ 'encryptedUnindexed': 'encrypted unindexed value' }");
      ASSERT (!mongoc_cursor_next (cursor, &got) && "expected one document to be returned, got more than one");

      mongoc_cursor_destroy (cursor);
      bson_destroy (&filter);
   }

   explicit_encryption_destroy (eef);
}

static void
test_explicit_encryption_case4 (void *unused)
{
   /* Case 4: can roundtrip encrypted indexed */
   bson_error_t error;
   bool ok;
   mongoc_client_encryption_encrypt_opts_t *eopts;
   bson_value_t plaintext = {0};
   bson_value_t payload;
   ee_fixture *eef = explicit_encryption_setup ();

   BSON_UNUSED (unused);

   plaintext.value_type = BSON_TYPE_UTF8;
   plaintext.value.v_utf8.str = "encrypted indexed value";
   plaintext.value.v_utf8.len = (uint32_t) strlen (plaintext.value.v_utf8.str);

   /* Use ``clientEncryption`` to encrypt the value "encrypted indexed value".
    */
   {
      eopts = mongoc_client_encryption_encrypt_opts_new ();
      mongoc_client_encryption_encrypt_opts_set_keyid (eopts, &eef->key1ID);
      mongoc_client_encryption_encrypt_opts_set_algorithm (eopts, MONGOC_ENCRYPT_ALGORITHM_INDEXED);
      mongoc_client_encryption_encrypt_opts_set_contention_factor (eopts, 0);

      ok = mongoc_client_encryption_encrypt (eef->clientEncryption, &plaintext, eopts, &payload, &error);
      ASSERT_OR_PRINT (ok, error);

      mongoc_client_encryption_encrypt_opts_destroy (eopts);
   }

   /* Use ``clientEncryption`` to decrypt ``payload`` */
   {
      bson_value_t got;

      ok = mongoc_client_encryption_decrypt (eef->clientEncryption, &payload, &got, &error);
      ASSERT_OR_PRINT (ok, error);
      ASSERT (got.value_type == BSON_TYPE_UTF8);
      ASSERT_CMPSTR (got.value.v_utf8.str, "encrypted indexed value");
      bson_value_destroy (&got);
   }

   bson_value_destroy (&payload);
   explicit_encryption_destroy (eef);
}

static void
test_explicit_encryption_case5 (void *unused)
{
   /* Case 5: can roundtrip encrypted unindexed */
   bson_error_t error;
   bool ok;
   mongoc_client_encryption_encrypt_opts_t *eopts;
   bson_value_t plaintext = {0};
   bson_value_t payload;
   ee_fixture *eef = explicit_encryption_setup ();

   BSON_UNUSED (unused);

   plaintext.value_type = BSON_TYPE_UTF8;
   plaintext.value.v_utf8.str = "encrypted unindexed value";
   plaintext.value.v_utf8.len = (uint32_t) strlen (plaintext.value.v_utf8.str);

   /* Use ``clientEncryption`` to encrypt the value "encrypted unindexed value".
    */
   {
      eopts = mongoc_client_encryption_encrypt_opts_new ();
      mongoc_client_encryption_encrypt_opts_set_keyid (eopts, &eef->key1ID);
      mongoc_client_encryption_encrypt_opts_set_algorithm (eopts, MONGOC_ENCRYPT_ALGORITHM_UNINDEXED);

      ok = mongoc_client_encryption_encrypt (eef->clientEncryption, &plaintext, eopts, &payload, &error);
      ASSERT_OR_PRINT (ok, error);

      mongoc_client_encryption_encrypt_opts_destroy (eopts);
   }

   /* Use ``clientEncryption`` to decrypt ``payload`` */
   {
      bson_value_t got;

      ok = mongoc_client_encryption_decrypt (eef->clientEncryption, &payload, &got, &error);
      ASSERT_OR_PRINT (ok, error);
      ASSERT (got.value_type == BSON_TYPE_UTF8);
      ASSERT_CMPSTR (got.value.v_utf8.str, "encrypted unindexed value");
      bson_value_destroy (&got);
   }

   bson_value_destroy (&payload);
   explicit_encryption_destroy (eef);
}

static void
_test_unique_index_on_keyaltnames_setup (void (*test_case) (mongoc_client_encryption_t *, const bson_value_t *keyid))
{
   mongoc_client_t *client = NULL;
   mongoc_database_t *keyvault = NULL;
   mongoc_client_encryption_t *client_encryption = NULL;
   bson_value_t existing_key;
   bson_error_t error;

   /* Create a MongoClient object (referred to as client). */
   client = test_framework_new_default_client ();
   keyvault = mongoc_client_get_database (client, "keyvault");

   /* Using client, drop the collection keyvault.datakeys. */
   {
      mongoc_collection_t *const datakeys = mongoc_database_get_collection (keyvault, "datakeys");
      mongoc_collection_drop (datakeys, &error);
      mongoc_collection_destroy (datakeys);
   }

   /* Using client, create a unique index on keyAltNames with a partial index
    * filter for only documents where keyAltNames exists. */
   {
      mongoc_collection_t *const datakeys = mongoc_database_get_collection (keyvault, "datakeys");

      mongoc_index_model_t *im =
         mongoc_index_model_new (tmp_bson ("{'keyAltNames': 1}"), tmp_bson (BSON_STR ({
                                    "name" : "keyAltNames_1",
                                    "unique" : true,
                                    "partialFilterExpression" : {"keyAltNames" : {"$exists" : true}}
                                 })));

      bson_t *const command_opts = tmp_bson ("{'writeConcern': { 'w': 'majority' }}");

      ASSERT_OR_PRINT (
         mongoc_collection_create_indexes_with_opts (datakeys, &im, 1, command_opts, NULL /* reply */, &error), error);
      mongoc_index_model_destroy (im);
      mongoc_collection_destroy (datakeys);
   }

   /* Create a ClientEncryption object (referred to as client_encryption) with
    * client set as the keyVaultClient. */
   {
      mongoc_client_encryption_opts_t *const client_encryption_opts = mongoc_client_encryption_opts_new ();
      bson_t *const kms_providers = _make_kms_providers (true /* aws */, true /* local */);
      bson_t *const tls_opts = _make_tls_opts ();

      mongoc_client_encryption_opts_set_kms_providers (client_encryption_opts, kms_providers);
      mongoc_client_encryption_opts_set_tls_opts (client_encryption_opts, tls_opts);
      mongoc_client_encryption_opts_set_keyvault_namespace (client_encryption_opts, "keyvault", "datakeys");
      mongoc_client_encryption_opts_set_keyvault_client (client_encryption_opts, client);
      client_encryption = mongoc_client_encryption_new (client_encryption_opts, &error);
      ASSERT_OR_PRINT (client_encryption, error);

      mongoc_client_encryption_opts_destroy (client_encryption_opts);
      bson_destroy (kms_providers);
      bson_destroy (tls_opts);
   }

   /* Using client_encryption, create a data key with a local KMS provider
    * and the keyAltName "def" (referred to as "the existing key"). */
   {
      mongoc_client_encryption_datakey_opts_t *const opts = mongoc_client_encryption_datakey_opts_new ();
      const char *const keyaltname[] = {"def"};

      mongoc_client_encryption_datakey_opts_set_keyaltnames (opts, (char **) keyaltname, 1u);

      ASSERT_OR_PRINT (
         mongoc_client_encryption_create_datakey (client_encryption, "local", opts, &existing_key, &error), error);

      mongoc_client_encryption_datakey_opts_destroy (opts);
   }

   test_case (client_encryption, &existing_key);

   mongoc_client_destroy (client);
   mongoc_database_destroy (keyvault);
   mongoc_client_encryption_destroy (client_encryption);
   bson_value_destroy (&existing_key);
}

static void
_test_unique_index_on_keyaltnames_case_1 (mongoc_client_encryption_t *client_encryption,
                                          const bson_value_t *existing_key)
{
   bson_error_t error;

   BSON_UNUSED (existing_key);

   /* Step 1: Use client_encryption to create a new local data key with a
    * keyAltName "abc" and assert the operation does not fail. */
   {
      mongoc_client_encryption_datakey_opts_t *const opts = mongoc_client_encryption_datakey_opts_new ();
      const char *const keyaltname[] = {"abc"};
      bson_value_t keyid;

      mongoc_client_encryption_datakey_opts_set_keyaltnames (opts, (char **) keyaltname, 1u);

      ASSERT_OR_PRINT (mongoc_client_encryption_create_datakey (client_encryption, "local", opts, &keyid, &error),
                       error);

      mongoc_client_encryption_datakey_opts_destroy (opts);
      bson_value_destroy (&keyid);
   }

   /* Step 2: Repeat Step 1 and assert the operation fails due to a duplicate
    * key server error (error code 11000). */
   {
      mongoc_client_encryption_datakey_opts_t *const opts = mongoc_client_encryption_datakey_opts_new ();
      const char *const keyaltname[] = {"abc"};
      bson_value_t keyid;

      mongoc_client_encryption_datakey_opts_set_keyaltnames (opts, (char **) keyaltname, 1u);

      ASSERT (!mongoc_client_encryption_create_datakey (client_encryption, "local", opts, &keyid, &error));
      ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_COLLECTION, MONGOC_ERROR_DUPLICATE_KEY, "keyAltNames: \"abc\"");

      mongoc_client_encryption_datakey_opts_destroy (opts);
      bson_value_destroy (&keyid);
   }

   /* Step 3: Use client_encryption to create a new local data key with a
    * keyAltName "def" and assert the operation fails due to a duplicate key
    * server error (error code 11000). */
   {
      mongoc_client_encryption_datakey_opts_t *const opts = mongoc_client_encryption_datakey_opts_new ();
      const char *const keyaltname[] = {"def"};
      bson_value_t keyid;

      mongoc_client_encryption_datakey_opts_set_keyaltnames (opts, (char **) keyaltname, 1u);

      ASSERT (!mongoc_client_encryption_create_datakey (client_encryption, "local", opts, &keyid, &error));
      ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_COLLECTION, MONGOC_ERROR_DUPLICATE_KEY, "keyAltNames: \"def\"");

      mongoc_client_encryption_datakey_opts_destroy (opts);
      bson_value_destroy (&keyid);
   }
}

static void
_test_unique_index_on_keyaltnames_case_2 (mongoc_client_encryption_t *client_encryption,
                                          const bson_value_t *existing_key)
{
   bson_value_t new_key;
   bson_error_t error;
   mongoc_client_encryption_datakey_opts_t *const opts = mongoc_client_encryption_datakey_opts_new ();

   /* Step 1: Use client_encryption to create a new local data key and assert
    * the operation does not fail. */
   ASSERT_OR_PRINT (mongoc_client_encryption_create_datakey (client_encryption, "local", opts, &new_key, &error),
                    error);

   /* Step 2: Use client_encryption to add a keyAltName "abc" to the key created
    * in Step 1 and assert the operation does not fail. */
   {
      bson_t key_doc;
      ASSERT_OR_PRINT (mongoc_client_encryption_add_key_alt_name (client_encryption, &new_key, "abc", &key_doc, &error),
                       error);
      bson_destroy (&key_doc);
   }

   /* Step 3: Repeat Step 2, assert the operation does not fail, and assert the
    * returned key document contains the keyAltName "abc" added in Step 2. */
   {
      bson_t key_doc;
      ASSERT_OR_PRINT (mongoc_client_encryption_add_key_alt_name (client_encryption, &new_key, "abc", &key_doc, &error),
                       error);
      ASSERT (match_bson (&key_doc, tmp_bson ("{'keyAltNames': ['abc']}"), false));
      bson_destroy (&key_doc);
   }

   /* Step 4: Use client_encryption to add a keyAltName "def" to the key created
    * in Step 1 and assert the operation fails due to a duplicate key server
    * error (error code 11000). */
   {
      bson_t key_doc;
      ASSERT (!mongoc_client_encryption_add_key_alt_name (client_encryption, &new_key, "def", &key_doc, &error));
      ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_QUERY, MONGOC_ERROR_DUPLICATE_KEY, "keyAltNames: \"def\"");
      bson_destroy (&key_doc);
   }

   /* Step 5: Use client_encryption to add a keyAltName "def" to the existing
    * key, assert the operation does not fail, and assert the returned key
    * document contains the keyAltName "def" added during Setup. */
   {
      bson_t key_doc;
      ASSERT_OR_PRINT (
         mongoc_client_encryption_add_key_alt_name (client_encryption, existing_key, "def", &key_doc, &error), error);
      ASSERT (match_bson (&key_doc, tmp_bson ("{'keyAltNames': ['def']}"), false));
      bson_destroy (&key_doc);
   }

   bson_value_destroy (&new_key);
   mongoc_client_encryption_datakey_opts_destroy (opts);
}

/* Prose Test 13: Unique Index on keyAltNames */
static void
test_unique_index_on_keyaltnames (void *unused)
{
   BSON_UNUSED (unused);

   _test_unique_index_on_keyaltnames_setup (_test_unique_index_on_keyaltnames_case_1);

   _test_unique_index_on_keyaltnames_setup (_test_unique_index_on_keyaltnames_case_2);
}

typedef struct {
   mongoc_client_t *setupClient;
   mongoc_client_t *encryptedClient;
   mongoc_collection_t *encryptedColl;
   bson_value_t ciphertext;
   bson_value_t malformedCiphertext;
   /* aggEvent is the CommandSucceeded or CommandFailed event observed for the
    * 'aggregate' command run in the test. */
   struct {
      const char *gotType; /* "none", "succeeded", or "failed" */
      bson_error_t gotFailedError;
      bson_t *gotSucceededReply;
   } aggEvent;
} decryption_events_fixture;

static void
decryption_events_succeeded_cb (const mongoc_apm_command_succeeded_t *event)
{
   decryption_events_fixture *def = (decryption_events_fixture *) mongoc_apm_command_succeeded_get_context (event);
   /* Only match the 'aggregate' command. */
   if (0 != strcmp (mongoc_apm_command_succeeded_get_command_name (event), "aggregate")) {
      return;
   }
   ASSERT_CMPSTR (def->aggEvent.gotType, "none");
   def->aggEvent.gotType = "succeeded";
   def->aggEvent.gotSucceededReply = bson_copy (mongoc_apm_command_succeeded_get_reply (event));
}

static void
decryption_events_failed_cb (const mongoc_apm_command_failed_t *event)
{
   decryption_events_fixture *def = (decryption_events_fixture *) mongoc_apm_command_failed_get_context (event);

   /* Only match the 'aggregate' command. */
   if (0 != strcmp (mongoc_apm_command_failed_get_command_name (event), "aggregate")) {
      return;
   }
   ASSERT_CMPSTR (def->aggEvent.gotType, "none");
   def->aggEvent.gotType = "failed";
   mongoc_apm_command_failed_get_error (event, &def->aggEvent.gotFailedError);
}

decryption_events_fixture *
decryption_events_setup (void)
{
   decryption_events_fixture *def = (decryption_events_fixture *) bson_malloc0 (sizeof (decryption_events_fixture));
   mongoc_client_encryption_t *clientEncryption;
   bson_value_t keyID;

   def->setupClient = test_framework_new_default_client ();
   def->aggEvent.gotType = "none";

   /* Drop and create the collection ``db.decryption_events`` */
   {
      mongoc_collection_t *coll = mongoc_client_get_collection (def->setupClient, "db", "decryption_events");
      bson_error_t error;

      if (!mongoc_collection_drop (coll, &error)) {
         if (error.code != MONGOC_SERVER_ERR_NS_NOT_FOUND) {
            test_error ("unexpected error in drop: %s", error.message);
         }
      }
      mongoc_collection_destroy (coll);
   }

   /* Create a ClientEncryption object */
   {
      mongoc_client_encryption_opts_t *ceOpts = mongoc_client_encryption_opts_new ();
      bson_t *kms_providers = _make_local_kms_provider (NULL);
      bson_error_t error;

      mongoc_client_encryption_opts_set_keyvault_client (ceOpts, def->setupClient);
      mongoc_client_encryption_opts_set_keyvault_namespace (ceOpts, "keyvault", "datakeys");
      mongoc_client_encryption_opts_set_kms_providers (ceOpts, kms_providers);

      clientEncryption = mongoc_client_encryption_new (ceOpts, &error);
      ASSERT_OR_PRINT (clientEncryption, error);

      bson_destroy (kms_providers);
      mongoc_client_encryption_opts_destroy (ceOpts);
   }

   /* Create a data key. */
   {
      mongoc_client_encryption_datakey_opts_t *dkOpts;
      bson_error_t error;
      bool res;

      dkOpts = mongoc_client_encryption_datakey_opts_new ();

      res = mongoc_client_encryption_create_datakey (clientEncryption, "local", dkOpts, &keyID, &error);
      ASSERT_OR_PRINT (res, error);

      mongoc_client_encryption_datakey_opts_destroy (dkOpts);
   }

   /* Create a valid ciphertext. */
   {
      mongoc_client_encryption_encrypt_opts_t *eOpts;
      bson_error_t error;
      bson_value_t plaintext;
      eOpts = mongoc_client_encryption_encrypt_opts_new ();
      plaintext.value_type = BSON_TYPE_UTF8;
      plaintext.value.v_utf8.str = "hello";

      const size_t len = strlen (plaintext.value.v_utf8.str);
      ASSERT (bson_in_range_unsigned (uint32_t, len));

      plaintext.value.v_utf8.len = (uint32_t) len;

      mongoc_client_encryption_encrypt_opts_set_algorithm (eOpts, MONGOC_AEAD_AES_256_CBC_HMAC_SHA_512_DETERMINISTIC);
      mongoc_client_encryption_encrypt_opts_set_keyid (eOpts, &keyID);

      ASSERT_OR_PRINT (mongoc_client_encryption_encrypt (clientEncryption, &plaintext, eOpts, &def->ciphertext, &error),
                       error);

      mongoc_client_encryption_encrypt_opts_destroy (eOpts);
   }

   /* Create a malformed ciphertext. */
   {
      bson_value_copy (&def->ciphertext, &def->malformedCiphertext);
      ASSERT (def->ciphertext.value_type == BSON_TYPE_BINARY);
      /* Change the last data byte to make malformed. The last data byte is
       * part of the HMAC tag. */
      def->malformedCiphertext.value.v_binary.data[def->malformedCiphertext.value.v_binary.data_len - 1]++;
   }

   /* Create a MongoClient with automatic decryption. */
   {
      mongoc_auto_encryption_opts_t *aeOpts = mongoc_auto_encryption_opts_new ();
      bson_t *kms_providers = _make_local_kms_provider (NULL);
      bson_error_t error;
      mongoc_uri_t *uri;

      mongoc_auto_encryption_opts_set_keyvault_namespace (aeOpts, "keyvault", "datakeys");
      mongoc_auto_encryption_opts_set_kms_providers (aeOpts, kms_providers);
      uri = test_framework_get_uri ();
      /* disable retryable reads so only one event is emitted on failure. */
      mongoc_uri_set_option_as_bool (uri, MONGOC_URI_RETRYREADS, false);
      def->encryptedClient = test_framework_client_new_from_uri (uri, NULL /* api */);
      test_framework_set_ssl_opts (def->encryptedClient);

      ASSERT (mongoc_client_set_error_api (def->encryptedClient, MONGOC_ERROR_API_VERSION_2));
      ASSERT_OR_PRINT (mongoc_client_enable_auto_encryption (def->encryptedClient, aeOpts, &error), error);

      def->encryptedColl = mongoc_client_get_collection (def->encryptedClient, "db", "decryption_events");

      bson_destroy (kms_providers);
      mongoc_auto_encryption_opts_destroy (aeOpts);
      mongoc_uri_destroy (uri);
   }

   /* Monitor for CommandSucceeded and CommandFailed events. */
   {
      mongoc_apm_callbacks_t *cbs = mongoc_apm_callbacks_new ();
      mongoc_apm_set_command_succeeded_cb (cbs, decryption_events_succeeded_cb);
      mongoc_apm_set_command_failed_cb (cbs, decryption_events_failed_cb);
      mongoc_client_set_apm_callbacks (def->encryptedClient, cbs, def);
      mongoc_apm_callbacks_destroy (cbs);
   }

   mongoc_client_encryption_destroy (clientEncryption);
   bson_value_destroy (&keyID);
   return def;
}

static void
decryption_events_fixture_destroy (decryption_events_fixture *def)
{
   mongoc_client_destroy (def->setupClient);
   mongoc_client_destroy (def->encryptedClient);
   mongoc_collection_destroy (def->encryptedColl);
   bson_value_destroy (&def->ciphertext);
   bson_value_destroy (&def->malformedCiphertext);
   bson_destroy (def->aggEvent.gotSucceededReply);
   bson_free (def);
}

/* Prose test 14: Case 1: Command Error */
/* test_decryption_events_command_error is a regression test for CDRIVER-4401.
 * Send a command on an encrypted client resulting in a { 'ok': 0 } reply.
 * Expect an error returned and a CommandFailed event to be emitted. */
static void
test_decryption_events_case1 (void *unused)
{
   bool got;
   bson_error_t error;
   decryption_events_fixture *def = decryption_events_setup ();
   const bson_t *found;
   mongoc_cursor_t *cursor;

   BSON_UNUSED (unused);

   got = mongoc_client_command_simple (def->setupClient,
                                       "admin",
                                       tmp_bson ("{'configureFailPoint': 'failCommand', 'mode': {'times': 1}, "
                                                 "'data': {'errorCode': 123, 'failCommands': ['aggregate']}}"),
                                       NULL /* read prefs */,
                                       NULL /* reply */,
                                       &error);
   ASSERT_OR_PRINT (got, error);

   cursor = mongoc_collection_aggregate (
      def->encryptedColl, MONGOC_QUERY_NONE, tmp_bson ("{}"), NULL /* opts */, NULL /* read prefs */);

   got = mongoc_cursor_next (cursor, &found);
   ASSERT_WITH_MSG (!got, "Expected error in mongoc_cursor_next, but got success");

   ASSERT (mongoc_cursor_error (cursor, &error));

   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_SERVER, 123, "failpoint");

   ASSERT_CMPSTR (def->aggEvent.gotType, "failed");
   ASSERT_ERROR_CONTAINS (def->aggEvent.gotFailedError, MONGOC_ERROR_SERVER, 123, "failpoint");
   ASSERT (!got);
   mongoc_cursor_destroy (cursor);
   decryption_events_fixture_destroy (def);
}

/* Prose test 14: Case 2: Network Error */
/* test_decryption_events_network_error is a regression test for CDRIVER-4401.
 * Send a command on an encrypted client resulting in a network error.
 * Expect an error returned and a CommandFailed event to be emitted. */
static void
test_decryption_events_case2 (void *unused)

{
   bool got;
   bson_error_t error;
   decryption_events_fixture *def = decryption_events_setup ();
   const bson_t *found;
   mongoc_cursor_t *cursor;

   BSON_UNUSED (unused);

   got = mongoc_client_command_simple (def->setupClient,
                                       "admin",
                                       tmp_bson ("{'configureFailPoint': 'failCommand', 'mode': {'times': 1}, "
                                                 "'data': {'errorCode': 123, 'closeConnection': true, "
                                                 "'failCommands': ['aggregate']}}"),
                                       NULL /* read prefs */,
                                       NULL /* reply */,
                                       &error);
   ASSERT_OR_PRINT (got, error);

   cursor = mongoc_collection_aggregate (
      def->encryptedColl, MONGOC_QUERY_NONE, tmp_bson ("{}"), NULL /* opts */, NULL /* read prefs */);
   got = mongoc_cursor_next (cursor, &found);
   ASSERT_WITH_MSG (!got, "Expected error in mongoc_cursor_next, but got success");
   ASSERT (mongoc_cursor_error (cursor, &error));
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_SOCKET, "socket error");

   ASSERT_CMPSTR (def->aggEvent.gotType, "failed");
   ASSERT_ERROR_CONTAINS (
      def->aggEvent.gotFailedError, MONGOC_ERROR_STREAM, MONGOC_ERROR_STREAM_SOCKET, "socket error");
   ASSERT (!got);
   mongoc_cursor_destroy (cursor);
   decryption_events_fixture_destroy (def);
}

/* Prose test 14: Case 3: Decrypt Error. */
/* test_decryption_events_decrypt_error is a regression test for CDRIVER-4401.
 * Decrypt a reply with a malformed ciphertext.
 * Expect an error returned and a CommandSucceeded event to be emitted with
 * ciphertext. */
static void
test_decryption_events_case3 (void *unused)
{
   bool got;
   bson_error_t error;
   decryption_events_fixture *def = decryption_events_setup ();
   bson_t to_insert = BSON_INITIALIZER;
   const bson_t *found;
   mongoc_cursor_t *cursor;

   BSON_UNUSED (unused);

   BSON_APPEND_VALUE (&to_insert, "encrypted", &def->malformedCiphertext);

   got = mongoc_collection_insert_one (def->encryptedColl, &to_insert, NULL /* opts */, NULL /* reply */, &error);
   ASSERT_OR_PRINT (got, error);

   cursor = mongoc_collection_aggregate (
      def->encryptedColl, MONGOC_QUERY_NONE, tmp_bson ("{}"), NULL /* opts */, NULL /* read prefs */);
   got = mongoc_cursor_next (cursor, &found);
   ASSERT_WITH_MSG (!got, "Expected error in mongoc_cursor_next, but got success");

   ASSERT (mongoc_cursor_error (cursor, &error));

   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_CLIENT_SIDE_ENCRYPTION, 1, "HMAC validation failure");

   ASSERT_CMPSTR (def->aggEvent.gotType, "succeeded");
   ASSERT_MATCH (def->aggEvent.gotSucceededReply,
                 "{ 'cursor' : { 'firstBatch' : [ { 'encrypted': { "
                 "'$$type': 'binData' }} ] } }");

   mongoc_cursor_destroy (cursor);
   bson_destroy (&to_insert);
   decryption_events_fixture_destroy (def);
}

/* Prose test 14: Case 4: Decrypt Success. */
/* test_decryption_events_decrypt_success is a regression test for CDRIVER-4401.
 * Decrypt a reply with a valid ciphertext.
 * Expect a successful return and a CommandSucceeded event to be emitted with
 * ciphertext. */
static void
test_decryption_events_case4 (void *unused)
{
   bool got;
   bson_error_t error;
   decryption_events_fixture *def = decryption_events_setup ();
   bson_t to_insert = BSON_INITIALIZER;
   const bson_t *found;
   mongoc_cursor_t *cursor;

   BSON_UNUSED (unused);

   BSON_APPEND_VALUE (&to_insert, "encrypted", &def->ciphertext);

   got = mongoc_collection_insert_one (def->encryptedColl, &to_insert, NULL /* opts */, NULL /* reply */, &error);
   ASSERT_OR_PRINT (got, error);

   cursor = mongoc_collection_aggregate (
      def->encryptedColl, MONGOC_QUERY_NONE, tmp_bson ("{}"), NULL /* opts */, NULL /* read prefs */);
   got = mongoc_cursor_next (cursor, &found);
   ASSERT_OR_PRINT (!mongoc_cursor_error (cursor, &error), error);
   ASSERT (got);

   ASSERT_CMPSTR (def->aggEvent.gotType, "succeeded");

   ASSERT_MATCH (def->aggEvent.gotSucceededReply,
                 "{ 'cursor' : { 'firstBatch' : [ { 'encrypted': { "
                 "'$$type': 'binData' }} ] } }");

   mongoc_cursor_destroy (cursor);
   bson_destroy (&to_insert);
   decryption_events_fixture_destroy (def);
}

static void
_test_rewrap_with_separate_client_encryption (const char *src_provider, const char *dst_provider)
{
   mongoc_uri_t *const uri = test_framework_get_uri ();
   mongoc_client_encryption_opts_t *const ce_opts = mongoc_client_encryption_opts_new ();
   mongoc_client_t *const src_client = test_framework_client_new_from_uri (uri, NULL);
   mongoc_client_t *const dst_client = test_framework_client_new_from_uri (uri, NULL);

   bson_error_t error = {0};
   bson_value_t keyid = {0};

   BSON_ASSERT (uri);
   BSON_ASSERT (ce_opts);
   BSON_ASSERT (src_client);
   BSON_ASSERT (dst_client);

   test_framework_set_ssl_opts (src_client);
   test_framework_set_ssl_opts (dst_client);

   {
      mongoc_client_encryption_opts_set_keyvault_client (ce_opts, src_client);
      mongoc_client_encryption_opts_set_keyvault_namespace (ce_opts, "keyvault", "datakeys");

      {
         bson_t *const kms_providers = _make_kms_providers (true, true);
         BSON_ASSERT (kms_providers);
         mongoc_client_encryption_opts_set_kms_providers (ce_opts, kms_providers);
         bson_destroy (kms_providers);
      }

      {
         bson_t *const tls_opts = _make_tls_opts ();
         BSON_ASSERT (tls_opts);
         mongoc_client_encryption_opts_set_tls_opts (ce_opts, tls_opts);
         bson_destroy (tls_opts);
      }
   }


   // 1. Drop the collection keyvault.datakeys.
   {
      mongoc_collection_t *datakeys = mongoc_client_get_collection (src_client, "keyvault", "datakeys");
      ASSERT (datakeys);
      (void) mongoc_collection_drop (datakeys, NULL);
      mongoc_collection_destroy (datakeys);
   }

   // 2. Create a ClientEncryption object named clientEncryption1 with these
   // options: (see ce_opts).
   mongoc_client_encryption_t *clientEncryption1 = mongoc_client_encryption_new (ce_opts, &error);
   ASSERT_OR_PRINT (clientEncryption1, error);

   // 3. Call clientEncryption1.createDataKey with srcProvider and these
   // options: (see below).
   {
      mongoc_client_encryption_datakey_opts_t *dk_opts = mongoc_client_encryption_datakey_opts_new ();

      {
         bson_t *const src_masterkey = _make_kms_masterkey (src_provider);
         BSON_ASSERT (src_masterkey);
         mongoc_client_encryption_datakey_opts_set_masterkey (dk_opts, src_masterkey);
         bson_destroy (src_masterkey);
      }

      ASSERT_OR_PRINT (
         mongoc_client_encryption_create_datakey (clientEncryption1, src_provider, dk_opts, &keyid, &error), error);

      mongoc_client_encryption_datakey_opts_destroy (dk_opts);
   }

   bson_value_t ciphertext = {0};

   // 4. Call clientEncryption1.encrypt with the value "test" and these options:
   // (see below).
   {
      char message[] = "test";

      bson_value_t bson_value;

      bson_value.value_type = BSON_TYPE_UTF8;
      bson_value.value.v_utf8.str = message;
      bson_value.value.v_utf8.len = (uint32_t) strlen (bson_value.value.v_utf8.str);

      mongoc_client_encryption_encrypt_opts_t *const e_opts = mongoc_client_encryption_encrypt_opts_new ();
      mongoc_client_encryption_encrypt_opts_set_keyid (e_opts, &keyid);
      mongoc_client_encryption_encrypt_opts_set_algorithm (e_opts, MONGOC_AEAD_AES_256_CBC_HMAC_SHA_512_DETERMINISTIC);
      ASSERT_OR_PRINT (mongoc_client_encryption_encrypt (clientEncryption1, &bson_value, e_opts, &ciphertext, &error),
                       error);
      mongoc_client_encryption_encrypt_opts_destroy (e_opts);
   }

   // 5. Create a ClientEncryption object named clientEncryption2 with these
   // options: (see ce_opts).
   mongoc_client_encryption_t *clientEncryption2 = mongoc_client_encryption_new (ce_opts, &error);
   ASSERT_OR_PRINT (clientEncryption2, error);

   mongoc_client_encryption_rewrap_many_datakey_result_t *const result =
      mongoc_client_encryption_rewrap_many_datakey_result_new ();
   BSON_ASSERT (result);

   // 6. Call clientEncryption2.rewrapManyDataKey with an empty filter and these
   // options: (see below).
   {
      {
         bson_t *const dst_masterkey = _make_kms_masterkey (dst_provider);
         BSON_ASSERT (dst_masterkey);
         ASSERT_OR_PRINT (mongoc_client_encryption_rewrap_many_datakey (
                             clientEncryption2, NULL, dst_provider, dst_masterkey, result, &error),
                          error);
         bson_destroy (dst_masterkey);
      }

      // Assert that the returned
      // RewrapManyDataKeyResult.bulkWriteResult.modifiedCount is 1.
      const bson_t *const bulk_write_result =
         mongoc_client_encryption_rewrap_many_datakey_result_get_bulk_write_result (result);
      ASSERT (bulk_write_result);
      ASSERT_WITH_MSG (match_bson (bulk_write_result, tmp_bson ("{'nModified': 1}"), false),
                       "'%s' does not match expected value",
                       tmp_json (bulk_write_result));
   }

   // 7. Call clientEncryption1.decrypt with the ciphertext. Assert the return
   // value is "test".
   {
      bson_value_t decrypted = {0};

      ASSERT (mongoc_client_encryption_decrypt (clientEncryption1, &ciphertext, &decrypted, &error));

      ASSERT (decrypted.value_type == BSON_TYPE_UTF8);
      ASSERT (decrypted.value.v_utf8.len != 0u);
      ASSERT_CMPSTR (decrypted.value.v_utf8.str, "test");

      bson_value_destroy (&decrypted);
   }

   // 8. Call clientEncryption2.decrypt with the ciphertext. Assert the return
   // value is "test".
   {
      bson_value_t decrypted = {0};

      ASSERT (mongoc_client_encryption_decrypt (clientEncryption2, &ciphertext, &decrypted, &error));

      ASSERT (decrypted.value_type == BSON_TYPE_UTF8);
      ASSERT (decrypted.value.v_utf8.len != 0u);
      ASSERT_CMPSTR (decrypted.value.v_utf8.str, "test");

      bson_value_destroy (&decrypted);
   }

   mongoc_client_encryption_rewrap_many_datakey_result_destroy (result);
   mongoc_client_encryption_destroy (clientEncryption2);
   bson_value_destroy (&ciphertext);
   bson_value_destroy (&keyid);
   mongoc_client_encryption_destroy (clientEncryption1);
   mongoc_client_encryption_opts_destroy (ce_opts);
   mongoc_uri_destroy (uri);
   mongoc_client_destroy (dst_client);
   mongoc_client_destroy (src_client);
}

/* Prose Test 16: Case 1: Rewrap with separate ClientEncryption */
static void
test_rewrap_with_separate_client_encryption (void *unused)
{
   BSON_UNUSED (unused);

   const char *const providers[] = {"aws", "azure", "gcp", "kmip", "local"};
   const size_t providers_len = sizeof (providers) / sizeof (*providers);

   for (size_t src_idx = 0u; src_idx < providers_len; ++src_idx) {
      for (size_t dst_idx = 0u; dst_idx < providers_len; ++dst_idx) {
         const char *const src = providers[src_idx];
         const char *const dst = providers[dst_idx];

         if (test_suite_debug_output ()) {
            printf ("  - %s -> %s\n", src, dst);
            fflush (stdout);
         }

         _test_rewrap_with_separate_client_encryption (src, dst);
      }
   }
}

/* Prose Test 16: Case 2: RewrapManyDataKeyOpts.provider is not optional. */
static void
test_rewrap_without_provider (void *unused)
{
   BSON_UNUSED (unused);

   mongoc_uri_t *const uri = test_framework_get_uri ();
   mongoc_client_encryption_opts_t *const ce_opts = mongoc_client_encryption_opts_new ();
   mongoc_client_t *const key_vault_client = test_framework_client_new_from_uri (uri, NULL);

   bson_error_t error = {0};

   BSON_ASSERT (uri);
   BSON_ASSERT (ce_opts);
   BSON_ASSERT (key_vault_client);

   test_framework_set_ssl_opts (key_vault_client);

   {
      mongoc_client_encryption_opts_set_keyvault_client (ce_opts, key_vault_client);
      mongoc_client_encryption_opts_set_keyvault_namespace (ce_opts, "keyvault", "datakeys");

      {
         bson_t *const kms_providers = _make_kms_providers (true, true);
         BSON_ASSERT (kms_providers);
         mongoc_client_encryption_opts_set_kms_providers (ce_opts, kms_providers);
         bson_destroy (kms_providers);
      }

      {
         bson_t *const tls_opts = _make_tls_opts ();
         BSON_ASSERT (tls_opts);
         mongoc_client_encryption_opts_set_tls_opts (ce_opts, tls_opts);
         bson_destroy (tls_opts);
      }
   }

   // 1. Create a ClientEncryption object named clientEncryption with these
   // options: (see ce_opts).
   mongoc_client_encryption_t *clientEncryption = mongoc_client_encryption_new (ce_opts, &error);
   ASSERT_OR_PRINT (clientEncryption, error);

   // 2. Call ``clientEncryption.rewrapManyDataKey`` with an empty ``filter``
   // and these options: (see below).
   {
      bool ok = mongoc_client_encryption_rewrap_many_datakey (
         clientEncryption, NULL /* filter */, NULL /* kms_provider */, tmp_bson ("{}"), NULL /* result */, &error);
      // Assert an error is returned from the driver suggesting that the
      // ``provider`` option is required.
      ASSERT_WITH_MSG (!ok, "expected error, but got success");
      ASSERT_ERROR_CONTAINS (error,
                             MONGOC_ERROR_CLIENT,
                             MONGOC_ERROR_CLIENT_INVALID_ENCRYPTION_ARG,
                             "expected 'provider' to be set to identify type of 'master_key'");
   }

   mongoc_client_encryption_destroy (clientEncryption);
   mongoc_client_encryption_opts_destroy (ce_opts);
   mongoc_uri_destroy (uri);
   mongoc_client_destroy (key_vault_client);
}

/* test_qe_docs_example tests the documentation example requested in
 * CDRIVER-4379. */
static void
test_qe_docs_example (void *unused)
{
   bson_error_t error;
   mongoc_client_t *const client = test_framework_new_default_client ();
   bson_t *const kmsProviders = _make_kms_providers (false /* with aws */, true /* with local */);
   bson_value_t key1ID;
   bson_value_t key2ID;
   bson_t *encryptedFieldsMap;
   mongoc_client_t *encryptedClient;
   mongoc_collection_t *encryptedColl;

   BSON_UNUSED (unused);

   /* Drop data from prior test runs. */
   {
      mongoc_database_t *db;
      mongoc_collection_t *coll;

      coll = mongoc_client_get_collection (client, "keyvault", "datakeys");
      if (!mongoc_collection_drop (coll, &error)) {
         bool ignored_error = NULL != strstr (error.message, "ns not found");
         ASSERT_OR_PRINT (ignored_error, error);
      }
      mongoc_collection_destroy (coll);

      db = mongoc_client_get_database (client, "docsExamples");
      ASSERT_OR_PRINT (mongoc_database_drop (db, &error), error);
      mongoc_database_destroy (db);
   }

   /* Create two data keys. */
   {
      mongoc_client_encryption_opts_t *ceOpts;
      mongoc_client_encryption_t *ce;
      mongoc_client_encryption_datakey_opts_t *dkOpts;

      ceOpts = mongoc_client_encryption_opts_new ();
      mongoc_client_encryption_opts_set_kms_providers (ceOpts, kmsProviders);
      mongoc_client_encryption_opts_set_keyvault_namespace (ceOpts, "keyvault", "datakeys");
      mongoc_client_encryption_opts_set_keyvault_client (ceOpts, client);
      ce = mongoc_client_encryption_new (ceOpts, &error);
      ASSERT_OR_PRINT (ce, error);

      dkOpts = mongoc_client_encryption_datakey_opts_new ();
      ASSERT_OR_PRINT (mongoc_client_encryption_create_datakey (ce, "local", dkOpts, &key1ID, &error), error);
      ASSERT_OR_PRINT (mongoc_client_encryption_create_datakey (ce, "local", dkOpts, &key2ID, &error), error);

      mongoc_client_encryption_datakey_opts_destroy (dkOpts);
      mongoc_client_encryption_destroy (ce);
      mongoc_client_encryption_opts_destroy (ceOpts);
   }

   /* Create an encryptedFieldsMap. */
   /* clang-format off */
   encryptedFieldsMap = BCON_NEW (
      "docsExamples.encrypted", "{",
         "fields", "[",
            "{",
               "path", "encryptedIndexed",
               "bsonType", "string",
               "keyId", BCON_BIN (key1ID.value.v_binary.subtype,
                                  key1ID.value.v_binary.data,
                                  key1ID.value.v_binary.data_len),
               "queries", "[", "{", "queryType", "equality", "}", "]",
            "}",
            "{",
               "path", "encryptedUnindexed",
               "bsonType", "string",
               "keyId", BCON_BIN (key2ID.value.v_binary.subtype,
                                  key2ID.value.v_binary.data,
                                  key2ID.value.v_binary.data_len),
            "}",
         "]",
      "}"
   );
   /* clang-format on */

   /* Create an Queryable Encryption collection. */
   {
      mongoc_auto_encryption_opts_t *aeOpts;
      mongoc_database_t *db;

      encryptedClient = test_framework_new_default_client ();
      aeOpts = mongoc_auto_encryption_opts_new ();
      mongoc_auto_encryption_opts_set_kms_providers (aeOpts, kmsProviders);
      mongoc_auto_encryption_opts_set_keyvault_namespace (aeOpts, "keyvault", "datakeys");
      mongoc_auto_encryption_opts_set_encrypted_fields_map (aeOpts, encryptedFieldsMap);
      ASSERT_OR_PRINT (mongoc_client_enable_auto_encryption (encryptedClient, aeOpts, &error), error);
      /* Create the Queryable Encryption collection docsExample.encrypted. */
      db = mongoc_client_get_database (encryptedClient, "docsExamples");
      /* Because docsExample.encrypted is in encryptedFieldsMap, it is created
       * with Queryable Encryption support. */
      encryptedColl = mongoc_database_create_collection (db, "encrypted", NULL /* opts */, &error);
      ASSERT_OR_PRINT (encryptedColl, error);

      mongoc_database_destroy (db);
      mongoc_auto_encryption_opts_destroy (aeOpts);
   }

   /* Auto encrypt an insert and find. */
   {
      /* Encrypt an insert. */
      bson_t *to_insert;
      bson_t *filter;
      const bson_t *got;
      mongoc_cursor_t *cursor;
      bool found;
      bson_iter_t iter;

      to_insert =
         BCON_NEW ("_id", BCON_INT32 (1), "encryptedIndexed", "indexedValue", "encryptedUnindexed", "unindexedValue");

      ASSERT_OR_PRINT (
         mongoc_collection_insert_one (encryptedColl, to_insert, NULL /* opts */, NULL /* reply */, &error), error);

      filter = BCON_NEW ("encryptedIndexed", "indexedValue");
      cursor = mongoc_collection_find_with_opts (encryptedColl, filter, NULL /* opts */, NULL /* read prefs */);
      found = mongoc_cursor_next (cursor, &got);
      ASSERT_OR_PRINT (!mongoc_cursor_error (cursor, &error), error);
      ASSERT (found);
      ASSERT (bson_iter_init_find (&iter, got, "encryptedIndexed"));
      ASSERT (BSON_ITER_HOLDS_UTF8 (&iter));
      ASSERT_CMPSTR (bson_iter_utf8 (&iter, NULL /* length */), "indexedValue");
      ASSERT (bson_iter_init_find (&iter, got, "encryptedUnindexed"));
      ASSERT (BSON_ITER_HOLDS_UTF8 (&iter));
      ASSERT_CMPSTR (bson_iter_utf8 (&iter, NULL /* length */), "unindexedValue");

      mongoc_cursor_destroy (cursor);
      bson_destroy (filter);
      bson_destroy (to_insert);
   }

   /* Find documents without decryption. */
   {
      mongoc_collection_t *unencryptedColl;
      bson_t *filter;
      const bson_t *got;
      mongoc_cursor_t *cursor;
      bool found;
      bson_iter_t iter;

      unencryptedColl = mongoc_client_get_collection (client, "docsExamples", "encrypted");
      filter = BCON_NEW ("_id", BCON_INT32 (1));
      cursor = mongoc_collection_find_with_opts (unencryptedColl, filter, NULL /* opts */, NULL /* read prefs */);
      found = mongoc_cursor_next (cursor, &got);
      ASSERT_OR_PRINT (!mongoc_cursor_error (cursor, &error), error);
      ASSERT (found);
      ASSERT (bson_iter_init_find (&iter, got, "encryptedIndexed"));
      ASSERT (BSON_ITER_HOLDS_BINARY (&iter));
      ASSERT (bson_iter_init_find (&iter, got, "encryptedUnindexed"));
      ASSERT (BSON_ITER_HOLDS_BINARY (&iter));

      mongoc_cursor_destroy (cursor);
      bson_destroy (filter);
      mongoc_collection_destroy (unencryptedColl);
   }

   mongoc_collection_destroy (encryptedColl);
   mongoc_client_destroy (encryptedClient);
   bson_destroy (encryptedFieldsMap);
   bson_value_destroy (&key2ID);
   bson_value_destroy (&key1ID);
   bson_destroy (kmsProviders);
   mongoc_client_destroy (client);
}

struct kms_callback_data {
   int value;
   const char *set_error;
   bool provide_creds;
};

static bool
_kms_callback (void *userdata, const bson_t *params, bson_t *out, bson_error_t *error)
{
   struct kms_callback_data *ctx = userdata;

   BSON_UNUSED (params);

   ctx->value = 42;
   if (ctx->set_error) {
      bson_set_error (error, MONGOC_ERROR_CLIENT_SIDE_ENCRYPTION, 1729, "%s", ctx->set_error);
      return false;
   }
   if (ctx->provide_creds) {
      uint8_t keydata[96] = {0};
      BCON_APPEND (out, "local", "{", "key", BCON_BIN (BSON_SUBTYPE_BINARY, keydata, sizeof keydata), "}");
   }
   return true;
}

static void
test_kms_callback (void *unused)
{
   // No interesting datakey options
   mongoc_client_encryption_datakey_opts_t *dk_opts = mongoc_client_encryption_datakey_opts_new ();

   // Create a client encryption object
   mongoc_client_encryption_opts_t *opts = mongoc_client_encryption_opts_new ();
   mongoc_client_t *cl = test_framework_new_default_client ();
   mongoc_client_encryption_opts_set_keyvault_client (opts, cl);

   BSON_UNUSED (unused);

   // Given it an on-demand 'local' provider
   bson_t *empty_local = tmp_bson ("{'local': {}}");
   mongoc_client_encryption_opts_set_kms_providers (opts, empty_local);
   mongoc_client_encryption_opts_set_keyvault_namespace (opts, "testing", "testing");

   {
      // Attempting to create a key from 'local' will fail immediately
      // Create a client encryption object for it.
      bson_error_t error;
      mongoc_client_encryption_t *enc = mongoc_client_encryption_new (opts, &error);
      ASSERT_OR_PRINT (enc, error);

      bson_value_t keyid;
      mongoc_client_encryption_create_datakey (enc, "local", dk_opts, &keyid, &error);
      mongoc_client_encryption_destroy (enc);

      ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_CLIENT_SIDE_ENCRYPTION, 1, "no kms provider set");
   }


   {
      // Now attach a callback
      struct kms_callback_data callback_data = {0};
      mongoc_client_encryption_opts_set_kms_credential_provider_callback (opts, _kms_callback, &callback_data);
      BSON_ASSERT (callback_data.value == 0);

      bson_error_t error;
      mongoc_client_encryption_t *enc = mongoc_client_encryption_new (opts, &error);
      ASSERT_OR_PRINT (enc, error);

      bson_value_t keyid;

      {
         mongoc_client_encryption_create_datakey (enc, "local", dk_opts, &keyid, &error);

         // The callback will have set a value when it was called
         BSON_ASSERT (callback_data.value == 42);

         // But we still get an error, because we didn't fill in 'local'
         ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_CLIENT_SIDE_ENCRYPTION, 1, "no kms provider set");
      }

      {
         // Now actually provide a key
         callback_data.provide_creds = true;
         ASSERT_OR_PRINT (mongoc_client_encryption_create_datakey (enc, "local", dk_opts, &keyid, &error), error);

         // The callback will have set a value when it was called
         BSON_ASSERT (callback_data.value == 42);
         bson_value_destroy (&keyid);
      }

      // Clear the value and tell the callback to set its own error
      callback_data.value = 0;
      callback_data.set_error = "This is the error that should appear from the callback";

      {
         mongoc_client_encryption_create_datakey (enc, "local", dk_opts, &keyid, &error);
         // It was called again:
         BSON_ASSERT (callback_data.value == 42);

         // This time the callback provided an error
         ASSERT_ERROR_CONTAINS (
            error, MONGOC_ERROR_CLIENT_SIDE_ENCRYPTION, 1729, "This is the error that should appear from the callback");
      }

      mongoc_client_encryption_destroy (enc);
   }

   mongoc_client_encryption_datakey_opts_destroy (dk_opts);
   mongoc_client_encryption_opts_destroy (opts);
   mongoc_client_destroy (cl);
}

static void
_test_auto_aws (bool should_succeed)
{
   // Datakey options for AWS
   mongoc_client_encryption_datakey_opts_t *dk_opts = mongoc_client_encryption_datakey_opts_new ();
   mongoc_client_encryption_datakey_opts_set_masterkey (dk_opts,
                                                        tmp_bson ("{ 'region': 'us-east-1', 'key': "
                                                                  "'arn:aws:kms:us-east-1:579766882180:key/"
                                                                  "89fcc2c4-08b0-4bd9-9f25-e30687b580d0' }"));

   // Create a client encryption object
   mongoc_client_encryption_opts_t *opts = mongoc_client_encryption_opts_new ();
   mongoc_client_t *cl = test_framework_new_default_client ();
   mongoc_client_encryption_opts_set_keyvault_client (opts, cl);

   // Given it an on-demand 'aws' provider
   bson_t *empty_aws = tmp_bson ("{'aws': {}}");
   mongoc_client_encryption_opts_set_kms_providers (opts, empty_aws);
   mongoc_client_encryption_opts_set_keyvault_namespace (opts, "testing", "testing");

   {
      // Attempting to create a key from 'aws' will require credentials in the
      // environment immediately. Create a client encryption object for it.
      bson_error_t error;
      mongoc_client_encryption_t *enc = mongoc_client_encryption_new (opts, &error);
      ASSERT_OR_PRINT (enc, error);

      bson_value_t keyid;
      mongoc_client_encryption_create_datakey (enc, "aws", dk_opts, &keyid, &error);
      mongoc_client_encryption_destroy (enc);

      if (should_succeed) {
         bson_value_destroy (&keyid);
         ASSERT_OR_PRINT (error.code == 0, error);
      } else {
         // We should encounter an error while attempting to connect to the EC2
         // metadata server.
         ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_CLIENT, MONGOC_ERROR_CLIENT_AUTHENTICATE, "");
      }
   }

   mongoc_client_encryption_datakey_opts_destroy (dk_opts);
   mongoc_client_encryption_opts_destroy (opts);
   mongoc_client_destroy (cl);
}

static void
test_auto_aws_fail (void *unused)
{
   BSON_UNUSED (unused);
   _test_auto_aws (false);
}

static void
test_auto_aws_succeed (void *unused)
{
   BSON_UNUSED (unused);
   _test_auto_aws (true);
}

static int
_have_aws_creds_env (void *unused)
{
   BSON_UNUSED (unused);

   // State variable:
   //    Zero: Haven't checked yet
   //    One: We have AWS creds
   //    Two = We do not have AWS creds
   static int creds_check_state = 0;
   if (creds_check_state == 0) {
      // We need to do a check
      _mongoc_aws_credentials_t creds = {0};
      bson_error_t error;
      bool got_creds = _mongoc_aws_credentials_obtain (NULL, &creds, &error);
      _mongoc_aws_credentials_cleanup (&creds);
      if (got_creds) {
         creds_check_state = 1;
      } else {
         creds_check_state = 2;
      }
   }
   return creds_check_state == 1;
}

static int
_not_have_aws_creds_env (void *unused)
{
   return !_have_aws_creds_env (unused);
}

// Test calling mongoc_collection_drop with a NULL bson_error_t when the state
// collections do not exist. This is a regression test for CDRIVER-4457.
static void
test_drop_qe_null_error (void *unused)
{
   bson_error_t error;
   mongoc_client_t *const client = test_framework_new_default_client ();
   bson_t *const kmsProviders = _make_kms_providers (false /* with aws */, true /* with local */);
   bson_t *encryptedFieldsMap;
   mongoc_client_t *encryptedClient;
   mongoc_auto_encryption_opts_t *aeOpts;
   mongoc_collection_t *coll;

   BSON_UNUSED (unused);

   /* Create an encryptedFieldsMap. */
   encryptedFieldsMap = BCON_NEW ("db.encrypted", "{", "fields", "[", "]", "}");
   encryptedClient = test_framework_new_default_client ();
   aeOpts = mongoc_auto_encryption_opts_new ();
   mongoc_auto_encryption_opts_set_kms_providers (aeOpts, kmsProviders);
   mongoc_auto_encryption_opts_set_keyvault_namespace (aeOpts, "keyvault", "datakeys");
   mongoc_auto_encryption_opts_set_encrypted_fields_map (aeOpts, encryptedFieldsMap);
   ASSERT_OR_PRINT (mongoc_client_enable_auto_encryption (encryptedClient, aeOpts, &error), error);
   coll = mongoc_client_get_collection (encryptedClient, "db", "encrypted");
   ASSERT (mongoc_collection_drop (coll, NULL));

   mongoc_collection_destroy (coll);
   mongoc_auto_encryption_opts_destroy (aeOpts);
   mongoc_client_destroy (encryptedClient);
   bson_destroy (encryptedFieldsMap);
   bson_destroy (kmsProviders);
   mongoc_client_destroy (client);
}

static bool
_auto_datakeys (struct auto_datakey_context *ctx)
{
   ctx->out_keyid->value.v_int32 = 42;
   ctx->out_keyid->value_type = BSON_TYPE_INT32;
   return true;
}

static bool
_auto_datakeys_error (struct auto_datakey_context *ctx)
{
   bson_set_error (ctx->out_error, 42, 1729, "I am an error");
   return false;
}

static bool
_auto_datakeys_error_noset (struct auto_datakey_context *ctx)
{
   // Do not set an error code, but indicate error anyway
   BSON_UNUSED (ctx);
   return false;
}

static void
test_auto_datakeys (void *unused)
{
   BSON_UNUSED (unused);

   bson_error_t error = {0};
   bson_t in_fields = BSON_INITIALIZER;
   bsonBuildArray ( //
      in_fields,
      doc (kv ("keyId", cstr ("keepme"))), //
      doc (kv ("keyId", null)));
   bson_t out_fields = BSON_INITIALIZER;
   bool okay = _mongoc_encryptedFields_fill_auto_datakeys (&out_fields, &in_fields, _auto_datakeys, NULL, &error);
   ASSERT_ERROR_CONTAINS (error, 0, 0, "");
   ASSERT (okay);
   bsonParse ( //
      out_fields,
      require (keyWithType ("0", doc), //
               parse (require (allOf (key ("keyId"), strEqual ("keepme")), nop))),
      require (keyWithType ("1", doc),
               parse (require (allOf (keyWithType ("keyId", int32)), do (ASSERT_CMPINT32 (bsonAs (int32), ==, 42))))));
   ASSERT (bsonParseError == NULL);
   bson_destroy (&out_fields);

   // Do it again, but we will generate an error
   okay = _mongoc_encryptedFields_fill_auto_datakeys (&out_fields, &in_fields, _auto_datakeys_error, NULL, &error);
   ASSERT (!okay);
   ASSERT_ERROR_CONTAINS (error, 42, 1729, "I am an error");
   bson_destroy (&out_fields);

   // Do it again, but we will generate an error without the factory setting the
   // error
   okay =
      _mongoc_encryptedFields_fill_auto_datakeys (&out_fields, &in_fields, _auto_datakeys_error_noset, NULL, &error);
   ASSERT (!okay);
   // Generic error, since the factory didn't provide one:
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_BSON, MONGOC_ERROR_BSON_INVALID, "indicated failure");
   bson_destroy (&out_fields);
   bson_destroy (&in_fields);
}

static void
_do_cec_test (void (*test) (const char *kmsProvider))
{
   test ("local");
   test ("aws");
}

// Declare a createEncryptedCollection test case (See usage below)
#define CEC_TEST(name, ...)               \
   static void name##_impl (__VA_ARGS__); \
   static void name (void *unused)        \
   {                                      \
      BSON_UNUSED (unused);               \
      _do_cec_test (name##_impl);         \
   }                                      \
   static void name##_impl (__VA_ARGS__)

CEC_TEST (test_create_encrypted_collection_simple, const char *kmsProvider)
{
   bson_error_t error = {0};
   mongoc_client_t *const client = test_framework_new_default_client ();
   bson_t *const kmsProviders = _make_kms_providers (true, true);
   bson_t *const tlsOptions = _make_tls_opts ();

   const char *const dbName = "cec-test-db";

   // Drop prior data
   {
      mongoc_collection_t *const coll = mongoc_client_get_collection (client, "keyvault", "datakeys");
      if (coll) {
         mongoc_collection_drop (coll, &error);
         bool okay = error.code == 0 || strstr (error.message, "ns not found") != NULL;
         ASSERT_OR_PRINT (okay, error);
      }
      mongoc_collection_destroy (coll);

      mongoc_database_t *const db = mongoc_client_get_database (client, dbName);
      ASSERT_OR_PRINT (mongoc_database_drop (db, &error), error);
      mongoc_database_destroy (db);
   }

   // Create a CE
   mongoc_client_encryption_opts_t *const ceOpts = mongoc_client_encryption_opts_new ();
   mongoc_client_encryption_opts_set_kms_providers (ceOpts, kmsProviders);
   mongoc_client_encryption_opts_set_tls_opts (ceOpts, tlsOptions);
   mongoc_client_encryption_opts_set_keyvault_namespace (ceOpts, "keyvault", "datakeys");
   mongoc_client_encryption_opts_set_keyvault_client (ceOpts, client);
   mongoc_client_encryption_t *const ce = mongoc_client_encryption_new (ceOpts, &error);
   mongoc_client_encryption_opts_destroy (ceOpts);
   ASSERT_OR_PRINT (ce, error);

   // Create the encrypted collection
   bsonBuildDecl (
      ccOpts,
      kv ("encryptedFields",
          doc (kv ("fields",
                   array (doc (kv ("path", cstr ("ssn")), kv ("bsonType", cstr ("string")), kv ("keyId", null)))))));
   mongoc_database_t *const db = mongoc_client_get_database (client, dbName);
   bson_t *const mkey = _make_kms_masterkey (kmsProvider);
   mongoc_collection_t *const coll = mongoc_client_encryption_create_encrypted_collection (
      ce, db, "test-coll", &ccOpts, NULL, kmsProvider, mkey, &error);
   ASSERT_OR_PRINT (coll, error);
   bson_destroy (&ccOpts);
   bson_destroy (mkey);

   bsonBuildDecl (doc, kv ("ssn", cstr ("123-45-6789")));
   const bool okay = mongoc_collection_insert_one (coll, &doc, NULL, NULL, &error);
   // Expect a failure: We didn't encrypt the field, and we don't have
   // auto-encryption enabled, but the server expects this to be encrypted.
   ASSERT (!okay);
   ASSERT_ERROR_CONTAINS (error, MONGOC_ERROR_COLLECTION, 121, "failed validation");
   bson_destroy (&doc);

   bson_destroy (kmsProviders);
   bson_destroy (tlsOptions);
   mongoc_collection_destroy (coll);
   mongoc_database_drop (db, &error);
   mongoc_database_destroy (db);
   mongoc_client_encryption_destroy (ce);
   mongoc_client_destroy (client);
}

static void
test_create_encrypted_collection_no_encryptedFields_helper (mongoc_client_t *client,
                                                            const char *dbName,
                                                            const char *collName,
                                                            const char *kmsProvider)
{
   bson_error_t error = {0};
   bson_t *const kmsProviders = _make_kms_providers (true, true);
   bson_t *const tlsOptions = _make_tls_opts ();

   ASSERT (client);

   // Drop prior data
   {
      mongoc_collection_t *const coll = mongoc_client_get_collection (client, "keyvault", "datakeys");
      if (coll) {
         mongoc_collection_drop (coll, &error);
         bool okay = error.code == 0 || strstr (error.message, "ns not found") != NULL;
         ASSERT_OR_PRINT (okay, error);
      }
      mongoc_collection_destroy (coll);

      mongoc_database_t *const db = mongoc_client_get_database (client, dbName);
      ASSERT_OR_PRINT (mongoc_database_drop (db, &error), error);
      mongoc_database_destroy (db);
   }

   // Create a CE
   mongoc_client_encryption_opts_t *const ceOpts = mongoc_client_encryption_opts_new ();
   mongoc_client_encryption_opts_set_kms_providers (ceOpts, kmsProviders);
   mongoc_client_encryption_opts_set_tls_opts (ceOpts, tlsOptions);
   mongoc_client_encryption_opts_set_keyvault_namespace (ceOpts, "keyvault", "datakeys");
   mongoc_client_encryption_opts_set_keyvault_client (ceOpts, client);
   mongoc_client_encryption_t *const ce = mongoc_client_encryption_new (ceOpts, &error);
   mongoc_client_encryption_opts_destroy (ceOpts);
   ASSERT_OR_PRINT (ce, error);

   // Create the encrypted collection
   bsonBuildDecl (ccOpts, do ());
   mongoc_database_t *const db = mongoc_client_get_database (client, dbName);
   bson_t *const mkey = _make_kms_masterkey (kmsProvider);
   mongoc_collection_t *const coll =
      mongoc_client_encryption_create_encrypted_collection (ce, db, collName, &ccOpts, NULL, kmsProvider, mkey, &error);
   ASSERT_ERROR_CONTAINS (
      error, MONGOC_ERROR_COMMAND, MONGOC_ERROR_COMMAND_INVALID_ARG, "No 'encryptedFields' are defined");
   bson_destroy (&ccOpts);
   bson_destroy (mkey);

   bson_destroy (kmsProviders);
   bson_destroy (tlsOptions);
   mongoc_collection_destroy (coll);
   mongoc_database_drop (db, &error);
   mongoc_database_destroy (db);
   mongoc_client_encryption_destroy (ce);
}

CEC_TEST (test_create_encrypted_collection_no_encryptedFields, const char *kmsProvider)
{
   const char *dbName = "cec-test-db";
   const char *collName = "test-coll";

   // Test with a default client.
   {
      mongoc_client_t *const client = test_framework_new_default_client ();
      test_create_encrypted_collection_no_encryptedFields_helper (client, dbName, collName, kmsProvider);
      mongoc_client_destroy (client);
   }

   // Test with a client configured with an encryptedFieldsMap.
   // This is not a required test. But a prior implementation checked if the
   // target collection was configured in encryptedFieldsMap.
   {
      mongoc_client_t *const client = test_framework_new_default_client ();
      mongoc_auto_encryption_opts_t *aeOpts = mongoc_auto_encryption_opts_new ();
      bson_t *const kmsProviders = _make_kms_providers (true /* with aws */, true /* with local */);
      char *namespace = bson_strdup_printf ("%s.%s", dbName, collName);
      bson_t *encryptedFieldsMap = tmp_bson ("{'%s': {'fields': []}}", namespace);
      bson_error_t error;

      mongoc_auto_encryption_opts_set_kms_providers (aeOpts, kmsProviders);
      mongoc_auto_encryption_opts_set_keyvault_namespace (aeOpts, "keyvault", "datakeys");
      mongoc_auto_encryption_opts_set_encrypted_fields_map (aeOpts, encryptedFieldsMap);
      ASSERT_OR_PRINT (mongoc_client_enable_auto_encryption (client, aeOpts, &error), error);

      test_create_encrypted_collection_no_encryptedFields_helper (client, dbName, collName, kmsProvider);

      bson_free (namespace);
      bson_destroy (kmsProviders);
      mongoc_auto_encryption_opts_destroy (aeOpts);
      mongoc_client_destroy (client);
   }
}

CEC_TEST (test_create_encrypted_collection_bad_keyId, const char *const kmsProvider)
{
   bson_error_t error = {0};
   mongoc_client_t *const client = test_framework_new_default_client ();
   bson_t *const kmsProviders = _make_kms_providers (true, true);
   bson_t *const tlsOptions = _make_tls_opts ();

   const char *const dbName = "cec-test-db";

   // Drop prior data
   {
      mongoc_collection_t *const coll = mongoc_client_get_collection (client, "keyvault", "datakeys");
      if (coll) {
         mongoc_collection_drop (coll, &error);
         bool okay = error.code == 0 || strstr (error.message, "ns not found") != NULL;
         ASSERT_OR_PRINT (okay, error);
      }
      mongoc_collection_destroy (coll);

      mongoc_database_t *const db = mongoc_client_get_database (client, dbName);
      ASSERT_OR_PRINT (mongoc_database_drop (db, &error), error);
      mongoc_database_destroy (db);
   }

   // Create a CE
   mongoc_client_encryption_opts_t *const ceOpts = mongoc_client_encryption_opts_new ();
   mongoc_client_encryption_opts_set_kms_providers (ceOpts, kmsProviders);
   mongoc_client_encryption_opts_set_tls_opts (ceOpts, tlsOptions);
   mongoc_client_encryption_opts_set_keyvault_namespace (ceOpts, "keyvault", "datakeys");
   mongoc_client_encryption_opts_set_keyvault_client (ceOpts, client);
   mongoc_client_encryption_t *const ce = mongoc_client_encryption_new (ceOpts, &error);
   mongoc_client_encryption_opts_destroy (ceOpts);
   ASSERT_OR_PRINT (ce, error);

   // Create the encrypted collection
   bsonBuildDecl (
      ccOpts,
      kv ("encryptedFields",
          doc (kv (
             "fields",
             array (doc (kv ("path", cstr ("ssn")), kv ("bsonType", cstr ("string")), kv ("keyId", bool (true))))))));
   mongoc_database_t *const db = mongoc_client_get_database (client, dbName);
   bson_t *const mkey = _make_kms_masterkey (kmsProvider);
   mongoc_collection_t *const coll = mongoc_client_encryption_create_encrypted_collection (
      ce, db, "test-coll", &ccOpts, NULL, kmsProvider, mkey, &error);
   ASSERT_ERROR_CONTAINS (
      error, MONGOC_ERROR_QUERY, MONGOC_ERROR_PROTOCOL_INVALID_REPLY, "create.encryptedFields.fields.keyId");
   bson_destroy (&ccOpts);
   bson_destroy (mkey);

   bson_destroy (kmsProviders);
   bson_destroy (tlsOptions);
   mongoc_collection_destroy (coll);
   mongoc_database_drop (db, &error);
   mongoc_database_destroy (db);
   mongoc_client_encryption_destroy (ce);
   mongoc_client_destroy (client);
}

// Implements Prose Test 21. Case: 4.
CEC_TEST (test_create_encrypted_collection_insert, const char *const kmsProvider)
{
   bson_error_t error = {0};
   mongoc_client_t *const client = test_framework_new_default_client ();
   bson_t *const kmsProviders = _make_kms_providers (true, true);
   bson_t *const tlsOptions = _make_tls_opts ();

   const char *const dbName = "cec-test-db";

   // Drop prior data
   {
      mongoc_collection_t *const coll = mongoc_client_get_collection (client, "keyvault", "datakeys");
      if (coll) {
         mongoc_collection_drop (coll, &error);
         bool okay = error.code == 0 || strstr (error.message, "ns not found") != NULL;
         ASSERT_OR_PRINT (okay, error);
      }
      mongoc_collection_destroy (coll);

      mongoc_database_t *const db = mongoc_client_get_database (client, dbName);
      ASSERT_OR_PRINT (mongoc_database_drop (db, &error), error);
      mongoc_database_destroy (db);
   }

   // Create a CE
   mongoc_client_encryption_opts_t *const ceOpts = mongoc_client_encryption_opts_new ();
   mongoc_client_encryption_opts_set_kms_providers (ceOpts, kmsProviders);
   mongoc_client_encryption_opts_set_tls_opts (ceOpts, tlsOptions);
   mongoc_client_encryption_opts_set_keyvault_namespace (ceOpts, "keyvault", "datakeys");
   mongoc_client_encryption_opts_set_keyvault_client (ceOpts, client);
   mongoc_client_encryption_t *const ce = mongoc_client_encryption_new (ceOpts, &error);
   mongoc_client_encryption_opts_destroy (ceOpts);
   ASSERT_OR_PRINT (ce, error);

   // Create the encrypted collection
   bsonBuildDecl (
      ccOpts,
      kv ("encryptedFields",
          doc (kv ("fields",
                   array (doc (kv ("path", cstr ("ssn")), kv ("bsonType", cstr ("string")), kv ("keyId", null)))))));
   mongoc_database_t *const db = mongoc_client_get_database (client, dbName);
   bson_t new_opts;
   bson_t *const mkey = _make_kms_masterkey (kmsProvider);
   mongoc_collection_t *const coll = mongoc_client_encryption_create_encrypted_collection (
      ce, db, "testing1", &ccOpts, &new_opts, kmsProvider, mkey, &error);
   ASSERT_OR_PRINT (coll, error);
   bson_destroy (&ccOpts);
   bson_destroy (mkey);

   // Extract the encryption key ID that was generated by
   // CreateEncryptedCollection:
   bson_value_t new_keyid;
   bsonParse (new_opts,
              require (keyWithType ("encryptedFields", doc),
                       parse (require (keyWithType ("fields", array),
                                       visitEach (require (type (doc)),
                                                  parse (require (key ("keyId"),
                                                                  require (type (binary)),
                                                                  do ({
                                                                     bson_value_copy (bson_iter_value ((
                                                                                         bson_iter_t *) &bsonVisitIter),
                                                                                      &new_keyid);
                                                                  }),
                                                                  halt)))))));
   ASSERT_CMPSTR (bsonParseError, NULL);

   // Generate some plaintext:
   bson_value_t plain;
   plain.value_type = BSON_TYPE_UTF8;
   plain.value.v_utf8.str = "123-45-6789";
   plain.value.v_utf8.len = (uint32_t) strlen (plain.value.v_utf8.str);

   // Encrypt the value using the new encryption key:
   mongoc_client_encryption_encrypt_opts_t *eo = mongoc_client_encryption_encrypt_opts_new ();
   mongoc_client_encryption_encrypt_opts_set_keyid (eo, &new_keyid);
   mongoc_client_encryption_encrypt_opts_set_algorithm (eo, MONGOC_ENCRYPT_ALGORITHM_UNINDEXED);
   bson_value_t ciphertext;
   bool okay = mongoc_client_encryption_encrypt (ce, &plain, eo, &ciphertext, &error);
   ASSERT_OR_PRINT (okay, error);
   mongoc_client_encryption_encrypt_opts_destroy (eo);
   bson_value_destroy (&new_keyid);

   // Insert the ciphertext:
   bsonBuildDecl (doc, kv ("ssn", value (ciphertext)));
   okay = mongoc_collection_insert_one (coll, &doc, NULL, NULL, &error);
   ASSERT_OR_PRINT (okay, error);
   // Success!

   bson_destroy (&doc);
   bson_value_destroy (&ciphertext);
   bson_destroy (kmsProviders);
   bson_destroy (tlsOptions);
   mongoc_collection_destroy (coll);
   mongoc_database_drop (db, &error);
   mongoc_database_destroy (db);
   mongoc_client_encryption_destroy (ce);
   mongoc_client_destroy (client);
   bson_destroy (&new_opts);
}


typedef struct listen_socket {
   mongoc_socket_t *socket;
   mongoc_cond_t cond;
   bson_mutex_t mutex;
   bool failed;
   char ip[16];
   unsigned short port;
   bool complete;
} listen_socket_args_t;

static BSON_THREAD_FUN (listen_socket, arg)
{
   listen_socket_args_t *args = arg;
   mongoc_socket_t *socket;
   struct sockaddr_in server_addr = {0};
   args->failed = false;

   // create a TcpListener on 127.0.0.1 endpoint
   socket = mongoc_socket_new (AF_INET, SOCK_STREAM, 0);
   BSON_ASSERT (socket);

   server_addr.sin_family = AF_INET;
   server_addr.sin_addr.s_addr = htonl (INADDR_LOOPBACK);
   server_addr.sin_port = htons (0);
   mongoc_socklen_t addr_len = (mongoc_socklen_t) sizeof server_addr;

   int r = mongoc_socket_bind (socket, (struct sockaddr *) &server_addr, addr_len);
   BSON_ASSERT (r == 0);

   // forward the port and ip for mongocryptdURI
   r = mongoc_socket_getsockname (socket, (struct sockaddr *) &server_addr, &addr_len);
   BSON_ASSERT (r == 0);

   bson_mutex_lock (&args->mutex);
   args->port = ntohs (server_addr.sin_port);
   inet_ntop (AF_INET, &server_addr.sin_addr, args->ip, sizeof (args->ip));
   mongoc_cond_signal (&args->cond);
   bson_mutex_unlock (&args->mutex);

   // listen on socket
   r = mongoc_socket_listen (socket, 100);
   BSON_ASSERT (r == 0);
   _mongoc_usleep (1000); // wait to see if received connection
   mongoc_socket_t *ret = mongoc_socket_accept (socket, bson_get_monotonic_time () + 100);
   if (ret) {
      // not null received a connection and test should fail
      args->failed = true;
   }
   // signal that test is complete.
   bson_mutex_lock (&args->mutex);
   args->complete = true;
   mongoc_cond_signal (&args->cond);
   bson_mutex_unlock (&args->mutex);

   mongoc_socket_destroy (socket);
   BSON_THREAD_RETURN;
}

/* Prose Test 20: Bypass creating mongocryptd client when shared library is
 * loaded */
static void
test_bypass_mongocryptd_shared_library (void *unused)
{
   BSON_UNUSED (unused);
   mongoc_client_t *client_encrypted;
   mongoc_auto_encryption_opts_t *auto_encryption_opts;
   bson_t *kms_providers;
   mongoc_database_t *db;
   mongoc_collection_t *coll;
   bson_error_t error;
   bson_thread_t socket_thread;

   // start the socket on a thread
   listen_socket_args_t *args = bson_malloc0 (sizeof (listen_socket_args_t));
   bson_mutex_init (&args->mutex);
   mongoc_cond_init (&args->cond);
   ASSERT_CMPINT (0, ==, mcommon_thread_create (&socket_thread, listen_socket, args));

   // configure mongoclient with auto encryption
   char *env_cryptSharedLibPath = test_framework_getenv ("MONGOC_TEST_CRYPT_SHARED_LIB_PATH");
   BSON_ASSERT (env_cryptSharedLibPath);
   auto_encryption_opts = mongoc_auto_encryption_opts_new ();
   kms_providers = BCON_NEW ("local", "{", "key", BCON_BIN (0, (uint8_t *) LOCAL_MASTERKEY, 96), "}");
   mongoc_auto_encryption_opts_set_kms_providers (auto_encryption_opts, kms_providers);
   mongoc_auto_encryption_opts_set_keyvault_namespace (auto_encryption_opts, "keyvault", "datakeys");

   // wait for port and ip to be set on the other thread
   bson_mutex_lock (&args->mutex);
   while (!args->port) {
      int cond_ret = mongoc_cond_timedwait (&args->cond, &args->mutex, 5000);
      /* ret non-zero indicates an error (a timeout) */
      BSON_ASSERT (!cond_ret);
   }
   bson_mutex_unlock (&args->mutex);
   BSON_ASSERT (strlen (args->ip) > 0);

   // configure extra options
   bson_t *extra = tmp_bson ("{'mongocryptdURI': 'mongodb://%s:%d', "
                             "'cryptSharedLibPath': '%s'}",
                             args->ip,
                             args->port,
                             env_cryptSharedLibPath);
   mongoc_auto_encryption_opts_set_extra (auto_encryption_opts, extra);
   bson_free (env_cryptSharedLibPath);
   // get the client
   client_encrypted = test_framework_new_default_client ();
   bool ret = mongoc_client_enable_auto_encryption (client_encrypted, auto_encryption_opts, &error);
   ASSERT_OR_PRINT (ret, error);

   // insert a document
   db = mongoc_client_get_database (client_encrypted, "db");
   coll = mongoc_database_get_collection (db, "coll");
   ret = mongoc_collection_insert_one (
      coll, tmp_bson ("{'unencrypted': 'test'}"), NULL /* opts */, NULL /* reply */, &error);
   ASSERT_OR_PRINT (ret, error);

   // Wait for listener thread to complete.
   bson_mutex_lock (&args->mutex);
   while (!args->complete) {
      int cond_ret = mongoc_cond_timedwait (&args->cond, &args->mutex, 5000);
      /* ret non-zero indicates an error (a timeout) */
      BSON_ASSERT (!cond_ret);
   }
   bson_mutex_unlock (&args->mutex);
   // failed should be false if the signal did not receive a connection
   BSON_ASSERT (!args->failed);
   mcommon_thread_join (socket_thread);

   bson_mutex_destroy (&args->mutex);
   mongoc_cond_destroy (&args->cond);
   bson_destroy (kms_providers);
   mongoc_auto_encryption_opts_destroy (auto_encryption_opts);
   mongoc_collection_destroy (coll);
   mongoc_database_destroy (db);
   mongoc_client_destroy (client_encrypted);
   bson_free (args);
}

void
test_client_side_encryption_install (TestSuite *suite)
{
   install_json_test_suite_with_check (suite,
                                       JSON_DIR,
                                       "client_side_encryption/legacy",
                                       test_client_side_encryption_cb,
                                       test_framework_skip_if_no_client_side_encryption);
   /* Prose tests from the spec. */
   TestSuite_AddFull (suite,
                      "/client_side_encryption/create_datakey_with_custom_key_material",
                      test_create_datakey_with_custom_key_material,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_client_side_encryption,
                      test_framework_skip_if_max_wire_version_less_than_8,
                      test_framework_skip_if_offline /* requires AWS */);
   TestSuite_AddFull (suite,
                      "/client_side_encryption/datakey_and_double_encryption",
                      test_datakey_and_double_encryption,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_client_side_encryption,
                      test_framework_skip_if_max_wire_version_less_than_8,
                      test_framework_skip_if_offline /* requires AWS */);
   TestSuite_AddFull (suite,
                      "/client_side_encryption/external_key_vault",
                      test_external_key_vault,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_client_side_encryption,
                      test_framework_skip_if_max_wire_version_less_than_8,
                      test_framework_skip_if_no_auth /* requires auth for error check */);
   TestSuite_AddFull (suite,
                      "/client_side_encryption/bson_size_limits_and_batch_splitting",
                      test_bson_size_limits_and_batch_splitting,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_client_side_encryption,
                      test_framework_skip_if_max_wire_version_less_than_8);
   TestSuite_AddFull (suite,
                      "/client_side_encryption/views_are_prohibited",
                      test_views_are_prohibited,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_client_side_encryption,
                      test_framework_skip_if_max_wire_version_less_than_8);
   TestSuite_AddFull (suite,
                      "/client_side_encryption/corpus",
                      test_corpus,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_client_side_encryption,
                      test_framework_skip_if_max_wire_version_less_than_8,
                      test_framework_skip_if_offline /* requires AWS */);
   TestSuite_AddFull (suite,
                      "/client_side_encryption/custom_endpoint",
                      test_custom_endpoint,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_client_side_encryption,
                      test_framework_skip_if_max_wire_version_less_than_8,
                      test_framework_skip_if_offline /* requires AWS, Azure, and GCP */);
   TestSuite_AddFull (suite,
                      "/client_side_encryption/bypass_spawning_mongocryptd/"
                      "mongocryptdBypassSpawn",
                      test_bypass_spawning_via_mongocryptdBypassSpawn,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_client_side_encryption,
                      test_framework_skip_if_max_wire_version_less_than_8);
   TestSuite_AddFull (suite,
                      "/client_side_encryption/bypass_spawning_mongocryptd/"
                      "bypassAutoEncryption",
                      test_bypass_spawning_via_bypassAutoEncryption,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_client_side_encryption,
                      test_framework_skip_if_max_wire_version_less_than_8);
   TestSuite_AddFull (suite,
                      "/client_side_encryption/bypass_spawning_mongocryptd/"
                      "bypassQueryAnalysis",
                      test_bypass_spawning_via_bypassQueryAnalysis,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_client_side_encryption,
                      test_framework_skip_if_max_wire_version_less_than_8);
   TestSuite_AddFull (suite,
                      "/client_side_encryption/bypass_spawning_mongocryptd/"
                      "cryptSharedLibLoaded",
                      test_bypass_spawning_via_cryptSharedLibLoaded,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_client_side_encryption,
                      test_framework_skip_if_max_wire_version_less_than_8,
                      _skip_if_no_crypt_shared);
   TestSuite_AddFull (suite,
                      "/client_side_encryption/kms_tls/valid",
                      test_kms_tls_cert_valid,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_client_side_encryption,
                      test_framework_skip_if_max_wire_version_less_than_8);
   TestSuite_AddFull (suite,
                      "/client_side_encryption/kms_tls/expired",
                      test_kms_tls_cert_expired,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_client_side_encryption,
                      test_framework_skip_if_max_wire_version_less_than_8);
   TestSuite_AddFull (suite,
                      "/client_side_encryption/kms_tls/wrong_host",
                      test_kms_tls_cert_wrong_host,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_client_side_encryption,
                      test_framework_skip_if_max_wire_version_less_than_8);
   TestSuite_AddFull (suite,
                      "/client_side_encryption/unique_index_on_keyaltnames",
                      test_unique_index_on_keyaltnames,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_client_side_encryption,
                      test_framework_skip_if_max_wire_version_less_than_8);
   TestSuite_AddFull (suite,
                      "/client_side_encryption/prose_test_16/case1",
                      test_rewrap_with_separate_client_encryption,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_client_side_encryption,
                      test_framework_skip_if_max_wire_version_less_than_8,
                      test_framework_skip_if_slow);
   TestSuite_AddFull (suite,
                      "/client_side_encryption/prose_test_16/case2",
                      test_rewrap_without_provider,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_client_side_encryption,
                      test_framework_skip_if_max_wire_version_less_than_8);

   /* Other, C driver specific, tests. */
   TestSuite_AddFull (suite,
                      "/client_side_encryption/single_and_pool_mismatches",
                      test_invalid_single_and_pool_mismatches,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_client_side_encryption,
                      test_framework_skip_if_max_wire_version_less_than_8);
   TestSuite_AddFull (suite,
                      "/client_side_encryption/multi_threaded",
                      test_multi_threaded,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_client_side_encryption,
                      test_framework_skip_if_max_wire_version_less_than_8);
   TestSuite_AddFull (suite,
                      "/client_side_encryption/malformed_explicit",
                      test_malformed_explicit,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_client_side_encryption,
                      test_framework_skip_if_max_wire_version_less_than_8);
   TestSuite_AddFull (suite,
                      "/client_side_encryption/kms_tls_options",
                      test_kms_tls_options,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_client_side_encryption,
                      test_framework_skip_if_max_wire_version_less_than_8,
                      test_framework_skip_if_offline /* requires AWS, Azure, and GCP */,
                      /* Do not run on Windows due to CDRIVER-4181. Tests use a literal IP with
                         a TLS connection. */
                      test_framework_skip_if_windows);

   TestSuite_AddFull (suite,
                      "/client_side_encryption/kms_tls_options/extra_rejected",
                      test_kms_tls_options_extra_rejected,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_client_side_encryption);

   TestSuite_AddFull (suite,
                      "/client_side_encryption/explicit_encryption/case1",
                      test_explicit_encryption_case1,
                      NULL /* dtor */,
                      NULL /* ctx */,
                      test_framework_skip_if_no_client_side_encryption,
                      test_framework_skip_if_max_wire_version_less_than_21,
                      // Remove skip_if_serverless once DRIVERS-2589 is resolved.
                      test_framework_skip_if_serverless,
                      test_framework_skip_if_single);

   TestSuite_AddFull (suite,
                      "/client_side_encryption/explicit_encryption/case2",
                      test_explicit_encryption_case2,
                      NULL /* dtor */,
                      NULL /* ctx */,
                      test_framework_skip_if_no_client_side_encryption,
                      test_framework_skip_if_max_wire_version_less_than_21,
                      // Remove skip_if_serverless once DRIVERS-2589 is resolved
                      test_framework_skip_if_serverless,
                      test_framework_skip_if_single);

   TestSuite_AddFull (suite,
                      "/client_side_encryption/explicit_encryption/case3",
                      test_explicit_encryption_case3,
                      NULL /* dtor */,
                      NULL /* ctx */,
                      test_framework_skip_if_no_client_side_encryption,
                      test_framework_skip_if_max_wire_version_less_than_21,
                      // Remove skip_if_serverless once DRIVERS-2589 is resolved.
                      test_framework_skip_if_serverless,
                      test_framework_skip_if_single);

   TestSuite_AddFull (suite,
                      "/client_side_encryption/explicit_encryption/case4",
                      test_explicit_encryption_case4,
                      NULL /* dtor */,
                      NULL /* ctx */,
                      test_framework_skip_if_no_client_side_encryption,
                      test_framework_skip_if_max_wire_version_less_than_21,
                      // Remove skip_if_serverless once DRIVERS-2589 is resolved.
                      test_framework_skip_if_serverless,
                      test_framework_skip_if_single);

   TestSuite_AddFull (suite,
                      "/client_side_encryption/explicit_encryption/case5",
                      test_explicit_encryption_case5,
                      NULL /* dtor */,
                      NULL /* ctx */,
                      test_framework_skip_if_no_client_side_encryption,
                      test_framework_skip_if_max_wire_version_less_than_21,
                      // Remove skip_if_serverless once DRIVERS-2589 is resolved.
                      test_framework_skip_if_serverless,
                      test_framework_skip_if_single);

   TestSuite_AddFull (suite,
                      "/client_side_encryption/decryption_events/case1",
                      test_decryption_events_case1,
                      NULL /* dtor */,
                      NULL /* ctx */,
                      test_framework_skip_if_no_client_side_encryption,
                      test_framework_skip_if_max_wire_version_less_than_8);

   TestSuite_AddFull (suite,
                      "/client_side_encryption/decryption_events/case2",
                      test_decryption_events_case2,
                      NULL /* dtor */,
                      NULL /* ctx */,
                      test_framework_skip_if_no_client_side_encryption,
                      test_framework_skip_if_max_wire_version_less_than_8);

   TestSuite_AddFull (suite,
                      "/client_side_encryption/decryption_events/case3",
                      test_decryption_events_case3,
                      NULL /* dtor */,
                      NULL /* ctx */,
                      test_framework_skip_if_no_client_side_encryption,
                      test_framework_skip_if_max_wire_version_less_than_8);


   TestSuite_AddFull (suite,
                      "/client_side_encryption/decryption_events/case4",
                      test_decryption_events_case4,
                      NULL /* dtor */,
                      NULL /* ctx */,
                      test_framework_skip_if_no_client_side_encryption,
                      test_framework_skip_if_max_wire_version_less_than_8);

   TestSuite_AddFull (suite,
                      "/client_side_encryption/qe_docs_example",
                      test_qe_docs_example,
                      NULL /* dtor */,
                      NULL /* ctx */,
                      test_framework_skip_if_no_client_side_encryption,
                      test_framework_skip_if_max_wire_version_less_than_21,
                      // Remove skip_if_serverless once DRIVERS-2589 is resolved.
                      test_framework_skip_if_serverless,
                      test_framework_skip_if_single);

   TestSuite_AddFull (suite,
                      "/client_side_encryption/kms/callback",
                      test_kms_callback,
                      NULL, // dtor
                      NULL, // ctx
                      test_framework_skip_if_no_client_side_encryption,
                      test_framework_skip_if_max_wire_version_less_than_8);

   TestSuite_AddFull (suite,
                      "/client_side_encryption/kms/auto-aws/fail",
                      test_auto_aws_fail,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_client_side_encryption,
                      test_framework_skip_if_max_wire_version_less_than_8,
                      _not_have_aws_creds_env);

   TestSuite_AddFull (suite,
                      "/client_side_encryption/kms/auto-aws/succeed",
                      test_auto_aws_succeed,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_client_side_encryption,
                      test_framework_skip_if_max_wire_version_less_than_8,
                      _have_aws_creds_env);

   TestSuite_AddFull (suite,
                      "/client_side_encryption/drop_qe_null_error",
                      test_drop_qe_null_error,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_client_side_encryption,
                      test_framework_skip_if_max_wire_version_less_than_8);

   TestSuite_AddFull (suite, "/client_side_encryption/auto_datakeys", test_auto_datakeys, NULL, NULL, NULL);

   TestSuite_AddFull (suite,
                      "/client_side_encryption/createEncryptedCollection/simple",
                      test_create_encrypted_collection_simple,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_client_side_encryption,
                      test_framework_skip_if_max_wire_version_less_than_21,
                      // Remove skip_if_serverless once DRIVERS-2589 is resolved.
                      test_framework_skip_if_serverless,
                      test_framework_skip_if_single);

   TestSuite_AddFull (suite,
                      "/client_side_encryption/createEncryptedCollection/"
                      "missing-encryptedFields",
                      test_create_encrypted_collection_no_encryptedFields,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_client_side_encryption,
                      test_framework_skip_if_max_wire_version_less_than_21,
                      // Remove skip_if_serverless once DRIVERS-2589 is resolved.
                      test_framework_skip_if_serverless,
                      test_framework_skip_if_single);
   TestSuite_AddFull (suite,
                      "/client_side_encryption/createEncryptedCollection/"
                      "bad-keyId",
                      test_create_encrypted_collection_bad_keyId,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_client_side_encryption,
                      test_framework_skip_if_max_wire_version_less_than_21,
                      // Remove skip_if_serverless once DRIVERS-2589 is resolved.
                      test_framework_skip_if_serverless,
                      test_framework_skip_if_single);
   TestSuite_AddFull (suite,
                      "/client_side_encryption/createEncryptedCollection/insert",
                      test_create_encrypted_collection_insert,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_client_side_encryption,
                      test_framework_skip_if_max_wire_version_less_than_21,
                      // Remove skip_if_serverless once DRIVERS-2589 is resolved.
                      test_framework_skip_if_serverless,
                      test_framework_skip_if_single);
   TestSuite_AddFull (suite,
                      "/client_side_encryption/bypass_mongocryptd_shared_library",
                      test_bypass_mongocryptd_shared_library,
                      NULL,
                      NULL,
                      test_framework_skip_if_no_client_side_encryption,
                      test_framework_skip_if_max_wire_version_less_than_17,
                      _skip_if_no_crypt_shared);

   // Add test cases for prose test: 22. Range Explicit Encryption.
   {
      const char *rangeTypes[] = {
         "DecimalNoPrecision",
         "DecimalPrecision",
         "DoubleNoPrecision",
         "DoublePrecision",
         "Date",
         "Int",
         "Long",
      };

      typedef struct {
         const char *name;
         TestFuncWC fn;
      } rangeCase;

      rangeCase rangeCases[] = {
         {"case1", test_range_explicit_encryption_case1},
         {"case2", test_range_explicit_encryption_case2},
         {"case3", test_range_explicit_encryption_case3},
         {"case4", test_range_explicit_encryption_case4},
         {"case5", test_range_explicit_encryption_case5},
         {"case6", test_range_explicit_encryption_case6},
         {"case7", test_range_explicit_encryption_case7},
         {"case8", test_range_explicit_encryption_case8},
      };

      for (size_t i = 0; i < sizeof rangeTypes / sizeof rangeTypes[0]; i++) {
         for (size_t j = 0; j < sizeof rangeCases / sizeof rangeCases[0]; j++) {
            const char *rangeType = rangeTypes[i];
            rangeCase rc = rangeCases[j];

            char *test_name =
               bson_strdup_printf ("/client_side_encryption/range_explicit_encryption/%s/%s", rc.name, rangeType);

            // Skip DecimalNoPrecision if not a replica set.
            if (0 == strcmp (rangeType, "DecimalNoPrecision")) {
               TestSuite_AddFull (suite,
                                  test_name,
                                  rc.fn,
                                  NULL /* dtor */,
                                  (void *) rangeTypes[i] /* ctx */,
                                  test_framework_skip_if_no_client_side_encryption,
                                  test_framework_skip_if_max_wire_version_less_than_21,
                                  test_framework_skip_if_max_wire_version_more_than_24 /* skip on MongoDB 8.0+ */,
                                  // Remove skip_if_serverless once DRIVERS-2589 is resolved.
                                  test_framework_skip_if_serverless,
                                  test_framework_skip_if_not_replset);
            } else {
               TestSuite_AddFull (suite,
                                  test_name,
                                  rc.fn,
                                  NULL /* dtor */,
                                  (void *) rangeTypes[i] /* ctx */,
                                  test_framework_skip_if_no_client_side_encryption,
                                  test_framework_skip_if_max_wire_version_less_than_21,
                                  test_framework_skip_if_max_wire_version_more_than_24 /* skip on MongoDB 8.0+ */,
                                  // Remove skip_if_serverless once DRIVERS-2589 is resolved.
                                  test_framework_skip_if_serverless,
                                  test_framework_skip_if_single);
            }

            bson_free (test_name);
         }
      }
   }
}
