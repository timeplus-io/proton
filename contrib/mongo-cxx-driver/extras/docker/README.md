# What is MongoDB?

[MongoDB](https://hub.docker.com/r/mongodb/mongodb-community-server) is a
cross-platform document-oriented NoSQL database. MongoDB uses JSON-like
documents with optional schemas. MongoDB is developed by
[MongoDB Inc](https://www.mongodb.com/).

This image provides both a C++ driver as well as a C driver which are used to
connect to MongoDB. The C++ driver is also known as
[mongo-cxx-driver](https://www.mongodb.com/docs/drivers/cxx/) and the C driver
is also known as
[mongo-c-driver](https://www.mongodb.com/docs/drivers/c/) or
[libmongoc](https://www.mongodb.com/docs/drivers/c/).

# Supported tags and respective Dockerfile links

## Tags

**Important**: the following tags are provided for development purposes only and do **NOT** receive security updates.

- [3.10.1-redhat-ubi-9.4](https://github.com/mongodb/mongo-cxx-driver/blob/5353ca8d092670fe80cc37424e666bbed43c0c2d/extras/docker/redhat-ubi-9.4/Dockerfile)
- [3.10.1-redhat-ubi-9.3](https://github.com/mongodb/mongo-cxx-driver/blob/0532dc531ef64521a5aa2339f5835bb2d9f6f964/extras/docker/redhat-ubi-9.3/Dockerfile)
- [3.10.0-redhat-ubi-9.3](https://github.com/mongodb/mongo-cxx-driver/blob/7fa047f64b476b7b7c422afc9b92eafde41f3412/extras/docker/redhat-ubi-9.3/Dockerfile)
- [3.9.0-redhat-ubi-9.3](https://github.com/mongodb/mongo-cxx-driver/blob/fe02b0dbb930d0fb28ca704bc5cfc0b31160d860/extras/docker/redhat-ubi-9.3/Dockerfile)
- [3.8.1-redhat-ubi-9.2](https://github.com/mongodb/mongo-cxx-driver/blob/5b1b515e1b355943003d72a04ae47a9e0e174374/extras/docker/redhat-ubi-9.2/Dockerfile)
- [3.8.0-redhat-ubi-9.2](https://github.com/mongodb/mongo-cxx-driver/blob/cb9dc3e927299bb9d2f2dc04878234e32c129685/extras/docker/redhat-ubi-9.2/Dockerfile)

# Examples

## C++ Driver Example Usage (mongo-cxx-driver)

First, get access to a MongoDB database server. The easiest way to do this is by
using [Atlas](https://www.mongodb.com/atlas/database), where you can run an `M0`
instance for
[free](https://www.mongodb.com/developer/products/atlas/free-atlas-cluster/).

Next, create a `Dockerfile` like so.
```Dockerfile
# Dockerfile
FROM mongodb/mongo-cxx-driver:3.9.0-redhat-ubi-9.3

WORKDIR /build

RUN microdnf upgrade -y && microdnf install -y g++

COPY ping.cpp /build/

RUN g++ \
    -o ping \
    ping.cpp \
    -I/usr/local/include/bsoncxx/v_noabi/ \
    -I/usr/local/include/mongocxx/v_noabi/ \
    -lmongocxx \
    -lbsoncxx

CMD /build/ping
```

Now let's create a simple program to ping the server. Let's name this program
`ping.cpp`. Notice that the connection string is stored as an environment
variable and is retrieved at runtime.
```C++
// ping.cpp
#include <cstdlib>
#include <string>

#include <bsoncxx/json.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>

std::string lookup_env(const std::string &name) {
  char *env = std::getenv(name.c_str());
  if (!env) {
    throw std::runtime_error("missing environment variable: " + name);
  }
  return env;
}

int main() {
  try {
    // Create an instance.
    mongocxx::instance inst{};

    std::string connection_string = lookup_env("MONGO_CONNECTION_STRING");

    const auto uri = mongocxx::uri{connection_string};

    // Set the version of the Stable API on the client.
    mongocxx::options::client client_options;
    const auto api = mongocxx::options::server_api{
        mongocxx::options::server_api::version::k_version_1};
    client_options.server_api_opts(api);

    // Setup the connection and get a handle on the "admin" database.
    mongocxx::client conn{uri, client_options};
    mongocxx::database db = conn["admin"];

    // Ping the database.
    const auto ping_cmd = bsoncxx::builder::basic::make_document(
        bsoncxx::builder::basic::kvp("ping", 1));
    db.run_command(ping_cmd.view());
    std::cout
        << "Pinged your deployment using the MongoDB C++ Driver. "
        << "You successfully connected to MongoDB!"
        << std::endl;
  } catch (const std::exception &e) {
    // Handle errors
    std::cerr << "Exception: " << e.what() << std::endl;
  }

  return 0;
}
```

Make sure that both `Dockerfile` and `ping.cpp` are in the same directory as
each other. For example, see the directory structure below:
```
$ tree .
.
├── Dockerfile
└── ping.cpp
```

Now we need to build the Docker image. Let's name this image `mongocxx-ping`
```sh
docker build . -t mongocxx-ping
```

We need to set the environment variable that contains the connection string for
our database. For an Atlas cluster, you can construct the connection string like
so:
```
'mongodb+srv://<your username here>:<your password here>@<database URL>/'
```

In Atlas, you can find your connection string by navigating to the `Database`
option underneath `Deployment`, then select `Connect`, click `Drivers`, then
select the `C++` driver. Remember to fill in the `<password>` parameter with
your actual password.

For further help in constructing a connection string, see the MongoDB
documentation
[here](https://www.mongodb.com/docs/manual/reference/connection-string/).

Now that we have constructed the connection string, run the command below with
said connection string.
```sh
docker run --env MONGO_CONNECTION_STRING='<your connection string here>' --rm mongocxx-ping
```

After running the previous line, you should see this output below:
```
Pinged your deployment using the MongoDB C++ Driver. You successfully connected to MongoDB!
```

## C Driver Example Usage (mongo-c-driver)

Because the C++ driver is a wrapper around the C driver, this image also
includes the MongoDB C driver.

First, get access to a MongoDB database server. The easiest way to do this is by
using [Atlas](https://www.mongodb.com/atlas/database), where you can run an `M0`
instance for
[free](https://www.mongodb.com/developer/products/atlas/free-atlas-cluster/).

Next, create a `Dockerfile` like so.
```Dockerfile
# Dockerfile
FROM mongodb/mongo-cxx-driver:3.9.0-redhat-ubi-9.3

WORKDIR /build

RUN microdnf upgrade -y && microdnf install -y gcc

COPY ping.c /build/

RUN gcc \
    -o ping \
    ping.c \
    -I/usr/local/include/libmongoc-1.0/ \
    -I/usr/local/include/libbson-1.0 \
    -L/usr/local/lib64/ \
    -lmongoc-1.0 \
    -lbson-1.0

CMD /build/ping
```

Now let's create a simple program to ping the server. Let's name this program
`ping.c`. Notice that the connection string is stored as an environment variable
and is retrieved at runtime.
```C
/* ping.c */
#include <mongoc/mongoc.h>

int main(void) {
    mongoc_client_t *client = NULL;
    bson_error_t error = {0};
    mongoc_server_api_t *api = NULL;
    mongoc_database_t *database = NULL;
    bson_t *command = NULL;
    bson_t reply = BSON_INITIALIZER;
    int rc = 0;
    bool ok = true;

    /* Initialize the MongoDB C Driver. */
    mongoc_init();

    const char *connection_string = getenv("MONGO_CONNECTION_STRING");
    if (!connection_string) {
        fprintf(
            stderr,
            "environment variable 'MONGO_CONNECTION_STRING' is missing\n"
        );
        rc = 1;
        goto cleanup;
    }

    client = mongoc_client_new(connection_string);
    if (!client) {
        fprintf(stderr, "failed to create a MongoDB client\n");
        rc = 1;
        goto cleanup;
    }

    /* Set the version of the Stable API on the client. */
    api = mongoc_server_api_new(MONGOC_SERVER_API_V1);
    if (!api) {
        fprintf(stderr, "failed to create a MongoDB server API\n");
        rc = 1;
        goto cleanup;
    }

    ok = mongoc_client_set_server_api(client, api, &error);
    if (!ok) {
        fprintf(stderr, "error: %s\n", error.message);
        rc = 1;
        goto cleanup;
    }

    /* Get a handle on the "admin" database. */
    database = mongoc_client_get_database(client, "admin");
    if (!database) {
        fprintf(stderr, "failed to get a MongoDB database handle\n");
        rc = 1;
        goto cleanup;
    }

    /* Ping the database. */
    command = BCON_NEW("ping", BCON_INT32(1));
    ok = mongoc_database_command_simple(
        database, command, NULL, &reply, &error
    );
    if (!ok) {
        fprintf(stderr, "error: %s\n", error.message);
        rc = 1;
        goto cleanup;
    }
    bson_destroy(&reply);

    printf(
        "Pinged your deployment using the MongoDB C Driver. "
        "You successfully connected to MongoDB!\n"
    );

cleanup:
    bson_destroy(command);
    mongoc_database_destroy(database);
    mongoc_server_api_destroy(api);
    mongoc_client_destroy(client);
    mongoc_cleanup();

    return rc;
}
```

Make sure that both `Dockerfile` and `ping.c` are in the same directory as each
other. For example, see the directory structure below:
```
$ tree .
.
├── Dockerfile
└── ping.c
```

Now we need to build the Docker image. Let's name this image `mongoc-ping`
```sh
docker build . -t mongoc-ping
```

We need to set the environment variable that contains the connection string for
our database. For an Atlas cluster, you can construct the connection string like
so:
```
'mongodb+srv://<your username here>:<your password here>@<database URL>/'
```

In Atlas, you can find your connection string by navigating to the `Database`
option underneath `Deployment`, then select `Connect`, click `Drivers`, then
select the `C` driver. Remember to fill in the `<password>` parameter with your
actual password.

For further help in constructing a connection string, see the MongoDB
documentation
[here](https://www.mongodb.com/docs/manual/reference/connection-string/).

Now that we have constructed the connection string, run the command below with
said connection string.
```sh
docker run --env MONGO_CONNECTION_STRING='<your connection string here>' --rm mongoc-ping
```

After running the previous line, you should see this output below:
```
Pinged your deployment using the MongoDB C Driver. You successfully connected to MongoDB!
```

# Further Reading

- [Documentation](https://www.mongodb.com/docs/drivers/cxx/) for mongo-cxx-driver
- [Documentation](https://www.mongodb.com/docs/drivers/c/) for mongo-c-driver

# License

[Apache License Version 2.0](https://github.com/mongodb/mongo-cxx-driver/blob/master/LICENSE)
