+++
date = "2016-08-15T16:11:58+05:30"
title = "Tutorial for mongocxx"
[menu.main]
  identifier = 'mongocxx-tutorial'
  weight = 11
  parent="mongocxx3"
+++

See the full code for this tutorial in
[tutorial.cpp](https://github.com/mongodb/mongo-cxx-driver/blob/master/examples/mongocxx/tutorial.cpp).

## Prerequisites

- A [mongod](https://www.mongodb.com/docs/manual/reference/program/mongod/)
  instance running on localhost on port 27017.

- The mongocxx Driver. See [Installation for mongocxx]({{< ref "/mongocxx-v3/installation" >}}).

- The following statements at the top of your source file:

```c++
#include <cstdint>
#include <iostream>
#include <vector>

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/json.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/stdx.hpp>
#include <mongocxx/uri.hpp>

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_array;
using bsoncxx::builder::basic::make_document;
```

## Compiling

The mongocxx driver's installation process will install a
`libmongocxx.pc` file for use
with [pkg-config](https://www.freedesktop.org/wiki/Software/pkg-config/).

To compile a program, run the following command:

```sh
c++ --std=c++11 <input>.cpp $(pkg-config --cflags --libs libmongocxx)
```

If you don't have pkg-config available, you will need to set include and
library flags manually on the command line or in your IDE.  For example, if
libmongoc and mongocxx are installed in `/usr/local`, then the compilation
line in above expands to this:

```sh
c++ --std=c++11 <input>.cpp
  -I/usr/local/include/mongocxx/v_noabi \
  -I/usr/local/include/bsoncxx/v_noabi \
  -L/usr/local/lib -lmongocxx -lbsoncxx
```

## Make a Connection

**IMPORTANT**: Before making any connections, you need to create one and only
one instance of [`mongocxx::instance`]({{< api3ref classmongocxx_1_1v__noabi_1_1instance >}}).
This instance must exist for the entirety of your program.

To connect to a running MongoDB instance, use the
[`mongocxx::client`]({{< api3ref classmongocxx_1_1v__noabi_1_1client >}})
class.

You must specify the host to connect to using a
[`mongocxx::uri`]({{< api3ref classmongocxx_1_1v__noabi_1_1uri >}}) instance containing a
[MongoDB URI](https://www.mongodb.com/docs/manual/reference/connection-string/),
and pass that into the `mongocxx::client` constructor.  For details regarding
supported URI options see the documentation for the version of libmongoc used
to build the C++ driver or for the [latest libmongoc release](
https://mongoc.org/libmongoc/current/mongoc_uri_t.html)

The default `mongocxx::uri` constructor will connect to a
server running on localhost on port `27017`:

```c++
mongocxx::instance instance{}; // This should be done only once.
mongocxx::client client{mongocxx::uri{}};
```

This is equivalent to the following:

```c++
mongocxx::instance instance{}; // This should be done only once.
mongocxx::uri uri("mongodb://localhost:27017");
mongocxx::client client(uri);
```

## Access a Database

Once you have a [`mongocxx::client`]({{< api3ref classmongocxx_1_1v__noabi_1_1client >}})
instance connected to a MongoDB deployment, use either the
`database()` method or `operator[]` to obtain a
[`mongocxx::database`]({{< api3ref classmongocxx_1_1v__noabi_1_1database >}})
instance.

If the database you request does not exist, MongoDB creates it when you
first store data.

The following example accesses the `mydb` database:

```c++
auto db = client["mydb"];
```

## Access a Collection

Once you have a
[`mongocxx::database`]({{< api3ref classmongocxx_1_1v__noabi_1_1database >}})
instance, use either the `collection()` method or `operator[]` to obtain a
[`mongocxx::collection`]({{< api3ref classmongocxx_1_1v__noabi_1_1collection >}})
instance.

If the collection you request does not exist, MongoDB creates it when
you first store data.

For example, using the `db` instance created in the previous section,
the following statement accesses the collection named `test` in the
`mydb` database:

```c++
auto collection = db["test"];
```

## Create a Document

To create a `document` using the C++ driver, use one of the two
available builder interfaces:

Stream builder: `bsoncxx::builder::stream`
  A document builder using the streaming operators that works well for
  literal document construction.

Basic builder: `bsoncxx::builder::basic`
  A more conventional document builder that involves calling methods on
  a builder instance.

This guide only briefly describes the basic builder.

For example, consider the following JSON document:

```json
{
   "name" : "MongoDB",
   "type" : "database",
   "count" : 1,
   "versions": [ "v6.0", "v5.0", "v4.4", "v4.2", "v4.0", "v3.6" ],
   "info" : {
               "x" : 203,
               "y" : 102
            }
}
```

Using the basic builder interface, you can construct this document
as follows:

```c++
auto doc_value = make_document(
    kvp("name", "MongoDB"),
    kvp("type", "database"),
    kvp("count", 1),
    kvp("versions", make_array("v6.0", "v5.0", "v4.4", "v4.2", "v4.0", "v3.6")),
    kvp("info", make_document(kvp("x", 203), kvp("y", 102))));
```

This `bsoncxx::document::value` type is a read-only object owning
its own memory. To use it, you must obtain a
[`bsoncxx::document::view`]({{< api3ref classbsoncxx_1_1v__noabi_1_1document_1_1view >}}) using
the `view()` method:

```c++
auto doc_view = doc_value.view();
```

You can access fields within this document view using `operator[]`,
which will return a
[`bsoncxx::document::element`]({{< api3ref classbsoncxx_1_1v__noabi_1_1document_1_1element >}})
instance. For example, the following will extract the `name` field whose
value is a string:

```c++
auto element = doc_view["name"];
assert(element.type() == bsoncxx::type::k_string);
auto name = element.get_string().value; // For C++ driver version < 3.7.0, use get_utf8()
assert(0 == name.compare("MongoDB"));
```

If the value in the `name` field is not a string and you do not
include a type guard as seen in the preceding example, this code will
throw an instance of
[`bsoncxx::exception`]({{< api3ref classbsoncxx_1_1v__noabi_1_1exception >}}).

## Insert Documents

### Insert One Document

To insert a single document into the collection, use a
[`mongocxx::collection`]({{< api3ref classmongocxx_1_1v__noabi_1_1collection >}})
instance's `insert_one()` method to insert `{ "i": 0 }`:

```c++
auto insert_one_result = collection.insert_one(make_document(kvp("i", 0)));
```

`insert_one_result` is an optional [`mongocxx::result::insert_one`]({{< api3ref
classmongocxx_1_1v__noabi_1_1result_1_1insert__one >}}). In this example, `insert_one_result`
is expected to be set. The default behavior for write operations is to wait for
a reply from the server. This may be overriden by setting an unacknowledged
[`mongocxx::write_concern`]({{< api3ref classmongocxx_1_1v__noabi_1_1write__concern >}}).

```c++
assert(insert_one_result);  // Acknowledged writes return results.
```

If you do not specify a top-level `_id` field in the document,
MongoDB automatically adds an `_id` field to the inserted document.

You can obtain this value using the `inserted_id()` method of the
returned
[`mongocxx::result::insert_one`]({{< api3ref classmongocxx_1_1v__noabi_1_1result_1_1insert__one >}})
instance.

```c++
auto doc_id = insert_one_result->inserted_id();
assert(doc_id.type() == bsoncxx::type::k_oid);
```

### Insert Multiple Documents

To insert multiple documents to the collection, use a
[`mongocxx::collection`]({{< api3ref classmongocxx_1_1v__noabi_1_1collection >}}) instance's
`insert_many()` method, which takes a list of documents to insert.

The following example inserts the documents `{ "i": 1 }` and `{ "i": 2 }`.
Create the documents and add to the documents list:

```c++
std::vector<bsoncxx::document::value> documents;
documents.push_back(make_document(kvp("i", 1)));
documents.push_back(make_document(kvp("i", 2)));
```

To insert these documents to the collection, pass the list of documents
to the `insert_many()` method.

```c++
auto insert_many_result = collection.insert_many(documents);
assert(insert_many_result);  // Acknowledged writes return results.
```

If you do not specify a top-level `_id` field in each document,
MongoDB automatically adds a `_id` field to the inserted documents.

You can obtain this value using the `inserted_ids()` method of the
returned
[`mongocxx::result::insert_many`]({{< api3ref classmongocxx_1_1v__noabi_1_1result_1_1insert__many >}})
instance.

```c++
auto doc0_id = insert_many_result->inserted_ids().at(0);
auto doc1_id = insert_many_result->inserted_ids().at(1);
assert(doc0_id.type() == bsoncxx::type::k_oid);
assert(doc1_id.type() == bsoncxx::type::k_oid);
```

## Query the Collection

To query the collection, use the collection’s `find()` and
`find_one` methods.

`find()` will return an instance of
[`mongocxx::cursor`]({{< api3ref classmongocxx_1_1v__noabi_1_1cursor >}}),
while `find_one()` will return an instance of
`std::optional<`[`bsoncxx::document::value`]({{< api3ref classbsoncxx_1_1v__noabi_1_1document_1_1value >}})`>`

You can call either method with an empty document to query all documents
in a collection, or pass a filter to query for documents that match the
filter criteria.

### Find a Single Document in a Collection

To return a single document in the collection, use the `find_one()`
method without any parameters.

```c++
auto find_one_result = collection.find_one({});
if (find_one_result) {
    // Do something with *find_one_result
}
assert(find_one_result);
```

### Find All Documents in a Collection

```c++
auto cursor_all = collection.find({});
for (auto doc : cursor_all) {
    // Do something with doc
    assert(doc["_id"].type() == bsoncxx::type::k_oid);
}
```

### Print All Documents in a Collection

The [`bsoncxx::to_json`]({{< api3ref namespacebsoncxx>}}) function converts a BSON document to a JSON string.

```c++
auto cursor_all = collection.find({});
std::cout << "collection " << collection.name()
          << " contains these documents:" << std::endl;
for (auto doc : cursor_all) {
    std::cout << bsoncxx::to_json(doc, bsoncxx::ExtendedJsonMode::k_relaxed) << std::endl;
}
std::cout << std::endl;
```

The above example prints output resembling the following:
```
collection test contains these documents:
{ "_id" : { "$oid" : "6409edb48c37f371c70f03a1" }, "i" : 0 }
{ "_id" : { "$oid" : "6409edb48c37f371c70f03a2" }, "i" : 1 }
{ "_id" : { "$oid" : "6409edb48c37f371c70f03a3" }, "i" : 2 }
```

The `_id` element has been added automatically by MongoDB to your
document and your value will differ from that shown. MongoDB reserves
field names that start with an underscore (`_`) and the dollar sign
(`$`) for internal use.

### Specify a Query Filter

#### Get A Single Document That Matches a Filter

To find the first document where the field `i` has the value `0`,
pass the document `{"i": 0}` to specify the equality condition:

```c++
auto find_one_filtered_result = collection.find_one(make_document(kvp("i", 0)));
if (find_one_filtered_result) {
    // Do something with *find_one_filtered_result
}
```

#### Get All Documents That Match a Filter

The following example gets all documents where `0 < "i" <= 2`:

```c++
auto cursor_filtered =
    collection.find(make_document(kvp("i", make_document(kvp("$gt", 0), kvp("$lte", 2)))));
for (auto doc : cursor_filtered) {
    // Do something with doc
    assert(doc["_id"].type() == bsoncxx::type::k_oid);
}
```

## Update Documents

To update documents in a collection, you can use the collection’s
`update_one()` and `update_many()` methods.

The update methods return an instance of
`std::optional<`[`mongocxx::result::update`]({{< api3ref classmongocxx_1_1v__noabi_1_1result_1_1update >}})`>`,
which provides information about the operation including the number of
documents modified by the update.

### Update a Single Document

To update at most one document, use the `update_one()` method.

The following example updates the first document that matches the filter
`{ "i": 0 }` and sets the value of `foo` to `bar`:

```c++
auto update_one_result =
    collection.update_one(make_document(kvp("i", 0)),
                          make_document(kvp("$set", make_document(kvp("foo", "bar")))));
assert(update_one_result);  // Acknowledged writes return results.
assert(update_one_result->modified_count() == 1);
```


### Update Multiple Documents

To update all documents matching a filter, use the `update_many()`
method.

The following example sets the value of `foo` to `buzz` where
`i` is greater than `0`:

```c++
auto update_many_result =
    collection.update_many(make_document(kvp("i", make_document(kvp("$gt", 0)))),
                            make_document(kvp("$set", make_document(kvp("foo", "buzz")))));
assert(update_many_result);  // Acknowledged writes return results.
assert(update_many_result->modified_count() == 2);
```

## Delete Documents

To delete documents from a collection, you can use a collection’s
`delete_one()` and `delete_many()` methods.

The delete methods return an instance of
`std::optional<`[`mongocxx::result::delete`]({{< api3ref classmongocxx_1_1v__noabi_1_1result_1_1delete__result >}})`>`,
which contains the number of documents deleted.

### Delete a Single Document

To delete at most a single document that matches a filter, use the
`delete_one()` method.

For example, to delete a document that matches the filter
`{ "i": 0 }`:

```c++
auto delete_one_result = collection.delete_one(make_document(kvp("i", 0)));
assert(delete_one_result);  // Acknowledged writes return results.
assert(delete_one_result->deleted_count() == 1);
```

### Delete All Documents That Match a Filter

To delete all documents matching a filter, use a collection's
`delete_many()` method.

The following example deletes all documents where `i` is greater than `0`:

```c++
auto delete_many_result =
    collection.delete_many(make_document(kvp("i", make_document(kvp("$gt", 0)))));
assert(delete_many_result);  // Acknowledged writes return results.
assert(delete_many_result->deleted_count() == 2);
```

## Create Indexes

To create an [index](https://www.mongodb.com/docs/manual/indexes/) on a
field or set of fields, pass an index specification document to the
`create_index()` method of a
[`mongocxx::collection`]({{< api3ref classmongocxx_1_1v__noabi_1_1collection >}}) instance. An
index key specification document contains the fields to index and the
index type for each field:

```json
{ "index1": "<type>", "index2": "<type>" }
```

- For an ascending index type, specify 1 for `<type>`.
- For a descending index type, specify -1 for `<type>`.

The following example creates an ascending index on the `i` field:

```c++
auto index_specification = make_document(kvp("i", 1));
collection.create_index(std::move(index_specification));
```
