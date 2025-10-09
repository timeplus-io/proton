+++
title = "Client-Side Field Level Encryption with mongocxx"
[menu.main]
  identifier = 'mongocxx-cse'
  weight = 11
  parent='mongocxx3'
+++

## Client-Side Field Level Encryption

New in MongoDB 4.2 [Client-Side Field Level Encryption (CSFLE)](https://www.mongodb.com/docs/drivers/use-cases/client-side-field-level-encryption-guide) allows administrators and developers to encrypt specific data fields in addition to other MongoDB encryption features.

With CSFLE, developers can encrypt fields client side without any server-side configuration or directives. Client-Side Field Level Encryption supports workloads where applications must guarantee that unauthorized parties, including server administrators, cannot read the encrypted data.

For an overview of CSFLE, please read [the official MongoDB documentation in the manual](https://www.mongodb.com/docs/manual/core/security-client-side-encryption/).

## Installation

### libmongocrypt

Client-Side Field Level Encryption relies on a C library called `libmongocrypt` to do the heavy lifting encryption work. This dependency is managed by the C driver.  As long as the C driver installation is 1.16.0 or higher, and has been compiled with Client-Side Field Level Encryption support, this dependency should be managed internally.  See the C driver's [Using Client-Side Field Level Encryption](https://mongoc.org/libmongoc/current/client-side-field-level-encryption.html) for more information.

### mongocryptd

Automatic CSFLE relies upon a new binary called `mongocryptd` running as a daemon while the driver operates.  This binary is only available with MongoDB Enterprise.

`mongocryptd` can either be started separately from the driver, or left to spawn automatically when encryption is used.

To run mongocryptd separately, pass the `mongocryptdBypassSpawn` flag to the client's auto encryption options:

```c++
auto mongocryptd_options = make_document(kvp("mongocryptdBypassSpawn", true));

options::auto_encryption auto_encrypt_opts{};
auto_encrypt_opts.extra_options({mongocryptd_options.view()});
```

If the mongocryptd binary is on the current path, the driver will be able to spawn it without any custom flags.  However, if the mongocryptd binary is on a different path, set the path with the `mongocryptdSpawnPath` option:

```c++
auto mongocryptd_options = make_document(kvp("mongocryptdSpawnPath", "path/to/mongocryptd"));

options::auto_encryption auto_encrypt_opts{};
auto_encrypt_opts.extra_options({mongocryptd_options.view()});
```

## Examples

### Automatic Client-Side Field Level Encryption

Automatic Client-Side Field Level Encryption is enabled by creating a `mongocxx::client` with the `auto_encryption_opts` option set to an instance of 'mongocxx::options::auto_encryption`. The following examples show how to set up automatic client-side field level encryption using the `mongocxx::client_encryption` class to create a new encryption data key.

{{% note %}}
Automatic client-side field level encryption requires MongoDB 4.2 enterprise or a MongoDB 4.2 Atlas cluster. The community version of the server supports automatic decryption as well as explicit client-side field level encryption.
{{% /note %}}

### Providing Local Automatic Encryption Rules

The following example shows how to specify automatic encryption rules via the schema_map option. The automatic encryption rules are expressed using a [strict subset of the JSON Schema syntax](https://www.mongodb.com/docs/manual/reference/security-client-side-automatic-json-schema/).

Supplying a `schema_map` provides more security than relying on JSON Schemas obtained from the server. It protects against a malicious server advertising a false JSON Schema, which could trick the client into sending unencrypted data that should be encrypted.

JSON Schemas supplied in the `schema_map` only apply to configuring automatic client-side field level encryption. Other validation rules in the JSON schema will not be enforced by the driver and will result in an error.

```c++
//
// The schema map has the following form:
//
//   {
//      "test.coll" : {
//         "bsonType" : "object",
//         "properties" : {
//            "encryptedFieldName" : {
//               "encrypt" : {
//                  "keyId" : [ <datakey as UUID> ],
//                  "bsonType" : "string",
//                  "algorithm" : <algorithm>
//               }
//            }
//         }
//      }
//   }
//
```

Please see [`examples/mongocxx/automatic_client_side_field_level_encryption.cpp`](https://github.com/mongodb/mongo-cxx-driver/blob/master/examples/mongocxx/automatic_client_side_field_level_encryption.cpp) for a full example of how to set a json schema for automatic encryption.

### Server-Side Field Level Encryption Enforcement

The MongoDB 4.2 server supports using schema validation to enforce encryption of specific fields in a collection. This schema validation will prevent an application from inserting unencrypted values for any fields marked with the `"encrypt"` JSON schema keyword.

It is possible to set up automatic client-side field level encryption using the `mongocxx::client_encryption` to create a new encryption data key and create a collection with the [Automatic Encryption JSON Schema Syntax](https://www.mongodb.com/docs/manual/reference/security-client-side-automatic-json-schema/).

```c++
// Please see the linked example below for full json_schema construction.
bsoncxx::document::value json_schema{};

// Create the collection with the encryption JSON Schema.
auto cmd = document{} << "create"
                      << "coll"
                      << "validator" << open_document
		      << "$jsonSchema" << json_schema.view()
                      << close_document << finalize;

db.run_command(cmd.view());
```

Please see [`examples/mongocxx/server_side_field_level_encryption_enforcement.cpp`](https://github.com/mongodb/mongo-cxx-driver/blob/master/examples/mongocxx/server_side_field_level_encryption_enforcement.cpp) for a full example of setting encryption enforcement on the server.

### Explicit Encryption

Explicit encryption is a MongoDB community feature and does not use the `mongocryptd` process. Explicit encryption is provided by the `mongocxx::client_encryption` class.

```c++
// Explicitly encrypt a BSON value.
auto to_encrypt = bsoncxx::types::bson_value::make_value("secret message");
auto encrypted_message = client_encryption.encrypt(to_encrypt, encrypt_opts);

// Explicitly decrypt a BSON value.
auto decrypted_message = client_encryption.decrypt(encrypted_message);
```

Please see [`examples/mongocxx/explicit_encryption.cpp`](https://github.com/mongodb/mongo-cxx-driver/blob/master/examples/mongocxx/explicit_encryption.cpp) for a full example of using explicit encryption and decryption.

### Explicit Encryption with Automatic Decryption

Although automatic encryption requires MongoDB 4.2 enterprise or a MongoDB 4.2 Atlas cluster, automatic decryption is supported for all users. To configure automatic decryption without automatic encryption, set `bypass_auto_encryption=True` in the `options::auto_encryption` class.

```c++
options::auto_encryption auto_encrypt_opts{};
auto_encrypt_opts.bypass_auto_encryption(true);
// Please see full example for complete options construction.

// Create a client with automatic decryption enabled, but automatic encryption bypassed.
options::client client_opts{};
client_opts.auto_encryption_opts(std::move(auto_encrypt_opts));
class client client_encrypted {uri{}, std::move(client_opts)};
```

Please see  [`examples/mongocxx/explicit_encryption_auto_decryption.cpp`](https://github.com/mongodb/mongo-cxx-driver/blob/master/examples/mongocxx/explicit_encryption_auto_decryption.cpp) for an example of using explicit encryption with automatic decryption.

