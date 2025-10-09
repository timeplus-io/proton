+++
date = "2016-08-15T16:11:58+05:30"
title = "Configuring the mongocxx driver"
[menu.main]
  weight = 10
  parent="mongocxx3"
+++

In the mongocxx driver, most configuration is done via the [connection
URI](https://www.mongodb.com/docs/manual/reference/connection-string/).  Some
additional connection options are possible via the
[mongocxx::options::client]({{< api3ref classmongocxx_1_1v__noabi_1_1options_1_1client
>}}) class.

## Configuring TLS/SSL

To enable TLS (SSL), set `tls=true` in the URI:

> `mongodb://mongodb.example.com/?tls=true`

By default, mongocxx will verify server certificates against the local
system CA list.  You can override that either by specifying different settings in
the connection string, or by creating a
[mongocxx::options::tls]({{< api3ref classmongocxx_1_1v__noabi_1_1options_1_1tls >}})
object and passing it to `tls_opts` on mongocxx::options::client.

For example, to use a custom CA or to disable certificate validation,
see the following example:

```cpp

// 1) Using tls_options
mongocxx::options::client client_options;
mongocxx::options::tls tls_options;

// If the server certificate is not signed by a well-known CA,
// you can set a custom CA file with the `ca_file` option.
tls_options.ca_file("/path/to/custom/cert.pem");

// If you want to disable certificate verification, you
// can set the `allow_invalid_certificates` option.
tls_options.allow_invalid_certificates(true);

client_options.tls_opts(tls_options);
auto client1 = mongocxx::client{uri{"mongodb://host1/?tls=true"}, client_options};

// 2) Using the URI
auto client2 = mongocxx::client{uri{"mongodb://host1/?tls=true&tlsAllowInvalidCertificates=true&tlsCAFile=/path/to/custom/cert.pem"}};
```

## Configuring authentication

### Default authentication mechanism

MongoDB 3.0 changed the default authentication mechanism from MONGODB-CR
to SCRAM-SHA-1. To create a credential that will authenticate properly
regardless of server version, use a connection string with the user and
password directly in the URI and with a parameter specifying the database
to authenticate from:

```cpp
#include <mongocxx/client.hpp>
#include <mongocxx/uri.hpp>

auto client = mongocxx::client{uri{"mongodb://user1:pwd1@host1/?authSource=db1"}};
```

### SCRAM-SHA-1

To explicitly create a credential of type SCRAM-SHA-1 use a connection
string as above but with a parameter specifying the authentication
mechanism as "SCRAM-SHA-1":

```cpp
#include <mongocxx/client.hpp>
#include <mongocxx/uri.hpp>

auto client = mongocxx::client{
    uri{"mongodb://user1:pwd1@host1/?authSource=db1&authMechanism=SCRAM-SHA-1"}};
```

### MONGODB-CR

The MONGODB-CR authMechanism is deprecated and will no longer function in MongoDB 4.0. Instead, specify no authMechanism and the driver
will use an authentication mechanism compatible with your server.

### X.509

The [X.509](https://www.mongodb.com/docs/manual/core/security-x.509/)
mechanism authenticates a user whose name is derived from the distinguished
subject name of the X.509 certificate presented by the driver during TLS
negotiation. This authentication method requires the use of TLS
connections with certificate validation and is available in MongoDB 2.6
and newer. To create a credential of this type, use a connection string with a
parameter that specifies the authentication mechanism as "MONGODB-X509",
that specifies the path to the PEM file containing the client private key
and certificate, and that has TLS enabled:

```cpp
#include <mongocxx/client.hpp>
#include <mongocxx/uri.hpp>

auto client = mongocxx::client{
    uri{"mongodb://host1/?authMechanism=MONGODB-X509&tlsCertificateFile=client.pem&tls=true"}};
```

See the MongoDB server
[X.509 tutorial](https://www.mongodb.com/docs/manual/tutorial/configure-x509-client-authentication/)
for more information about determining the subject name from the
certificate.

The PEM file can also be specified using the [mongocxx::options::tls]({{< api3ref classmongocxx_1_1v__noabi_1_1options_1_1tls >}}) class, see the first "Configuring TLS/SSL" example above.

### Kerberos (GSSAPI)

[MongoDB Enterprise](https://www.mongodb.com/products/mongodb-enterprise)
supports proxy authentication through Kerberos service. To create a
credential of type [Kerberos (GSSAPI)](https://www.mongodb.com/docs/manual/core/kerberos/)
use a connection string with the username and realm in the URI as well as
a parameter specifying the authentication mechanism as "GSSAPI":

```cpp
#include <mongocxx/client.hpp>
#include <mongocxx/uri.hpp>

auto client = mongocxx::client{
    uri{"mongodb://username%40REALM.COM@host1/?authMechanism=GSSAPI"}};
```

Note that the "@" symbol in the URI must be escaped to "%40" as shown in the example above.

### LDAP

[MongoDB Enterprise](https://www.mongodb.com/products/mongodb-enterprise)
supports proxy authentication through a Lightweight Directory Access
Protocol (LDAP) service. To create a credential of type LDAP use a
connection string specifying the user as well as parameters specifying
the authentication source as "$external" and the authentication mechanishm
as "PLAIN":

```cpp
#include <mongocxx/client.hpp>
#include <mongocxx/uri.hpp>

auto client = mongocxx::client{
    uri{"mongodb://user1:pwd1@host1/?authSource=$external&authMechanism=PLAIN"}};
```

## Configuring a connection pool

To configure a connection pool, first create a `mongocxx::pool`, passing
the URI as an argument. The size of the pool can be configured in the URI.
Then, call `mongocxx::pool::acquire` to receive a client from the pool.
The client will automatically be returned to the pool when it goes out of
scope.

```cpp
#include <mongocxx/pool.hpp>
#include <mongocxx/uri.hpp>

auto pool = mongocxx::pool{uri{"mongodb://host1/?minPoolSize=3&maxPoolSize=5"}};

{
    // To get a client from the pool, call `acquire()`.
    auto client = pool.acquire();

    // The client is returned to the pool when it goes out of scope.
}
```
 
See [connection pool documentation](../connection-pools) for more details.

## Compressing data to and from MongoDB

MongoDB 3.4 added Snappy compression support, while zlib compression was added
in 3.6, and zstd compression in 4.2.

Data compression can be enabled with the appropriate URI options, as documented
in [the C driver](https://mongoc.org/libmongoc/current/data-compression.html).
