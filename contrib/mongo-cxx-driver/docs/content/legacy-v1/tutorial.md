+++
date = "2016-08-15T16:11:58+05:30"
title = "Legacy driver tutorial"
[menu.main]
  identifier = 'legacy-tutorial'
  weight = 15
  parent="legacy"
+++

### Getting started with the Legacy C++ Driver

This is an introduction to usage of the MongoDB database from a C++ program.

NOTE: this tutorial is for the legacy and 26compat versions of the C++ driver.

First, install MongoDB – see the
[installation](https://www.mongodb.com/docs/manual/installation/) page for
details.

Next, you may wish to take a look at the [MongoDB
Manual](https://www.mongodb.com/docs/manual/) for a language independent look at
how to use MongoDB. Also, we suggest some basic familiarity with the [mongo
shell](https://www.mongodb.com/docs/mongodb-shell/) – the shell is the primary
database administration tool and is useful for manually inspecting the
contents of a database after your C++ program runs.

### Installing the Driver Library and Headers

Please see [Installation]({{< ref "/legacy-v1/installation" >}}) for
instructions on how to download, build, and install the C++ client driver.

### Initializing the Driver Library

Please see [Configuration]({{< ref "/legacy-v1/configuration" >}}) for
instructions on how to properly initialize and terminate the driver:

### Connecting

#### DBClientConnection

The C++ driver includes several classes for managing collections under the parent class `DBClientInterface`.

 - `DBClientConnection` is the connection class for connecting to a single MongoDB database server (or mongos)
 - `DBClientReplicaSet` is the connection class for connecting to a replica set.

See the [API documentation](https://mongodb.github.io/api/1.0.2/) for
details on each of the above classes.

#### A simple program that connects to the database

```cpp
#include <cstdlib>
#include <iostream>
#include "mongo/client/dbclient.h" // for the driver

void run() {
  mongo::DBClientConnection c;
  c.connect("localhost");
}

int main() {
    mongo::client::initialize();
    try {
        run();
        std::cout << "connected ok" << std::endl;
    } catch( const mongo::DBException &e ) {
        std::cout << "caught " << e.what() << std::endl;
    }
    return EXIT_SUCCESS;
}
```

(Note that in a production environment, the return value of `mongo::client::initialize()` must be checked)

If you are using gcc on Linux, you would compile with something like this, depending on location of your include files and libraries:

```sh
$ g++ tutorial.cpp -pthread -lmongoclient -lboost_thread-mt -lboost_system -lboost_regex -o tutorial
$ ./tutorial
connected ok
```

> **Warning**

 - Since the tutorial program attempts to connect to a MongoDB database server, you must start it by running mongod before running the tutorial.
 - You may need to append -mt to boost_filesystem and boost_program_options. If using a recent boost, -mt is not needed anymore.
 - You may need to use -I and -L to specify the locations of your mongo and boost headers and libraries.
 - If using the 26compat branch you need to additionally specify `-lboost_filesystem` and `-lboost_program_options`

### BSON

The MongoDB database stores data in BSON format. BSON is a binary object
format that is JSON-like in terms of the data which can be stored (some
extensions exist, for example, a Date datatype).

To save data in the database we must create objects of class BSONObj. The
components of a BSONObj are represented as BSONElement objects. We use
BSONObjBuilder to make BSON objects, and BSONObjIterator to enumerate BSON
objects.

#### The C++ BSON Library

Include bson/bson.h in your application. See
[bsondemo](https://github.com/mongodb/mongo-cxx-driver/blob/legacy/src/mongo/bson/bsondemo/bsondemo.cpp)
for example usage.

##### Key classes

```cpp
mongo::BSONObj: a BSON object
mongo::BSONElement: a single element in a BSON object. This is a key and a value.
mongo::BSONObjBuilder: used to make BSON objects
mongo::BSONObjIterator: used to enumerate BSON objects
```

##### Working with BSON

Let’s now create a BSON “person” object which contains name and age. We might invoke:

```cpp
BSONObjBuilder b;
b.append("name", "Joe");
b.append("age", 33);
BSONObj p = b.obj();
```

Or more concisely:

```cpp
BSONObj p = BSONObjBuilder().append("name", "Joe").append("age", 33).obj();
```

We can also create BSON objects using the stream oriented syntax:

```cpp
BSONObjBuilder b;
b << "name" << "Joe" << "age" << 33;
BSONObj p = b.obj();
```

The BSON Macro lets us be even more compact:

```cpp
BSONObj p = BSON( "name" << "Joe" << "age" << 33 );
```

Use the GENOID helper to add an object id to your object. The server will
add an \_id automatically if it is not included explicitly.

```cpp
BSONObj p = BSON( GENOID << "name" << "Joe" << "age" << 33 );
// result is: { _id : ..., name : "Joe", age : 33 }
```

GENOID should be at the beginning of the generated object. We can do something similar with the non-stream builder syntax:

```cpp
BSONObj p = BSONObjBuilder().genOID().append("name","Joe").append("age",33).obj();
```

Other helpers are listed in [Working with BSON]({{< ref "/legacy-v1/working-with-bson" >}}).

### Inserting

We now save our person object in a persons collection in the database:

```cpp
c.insert("tutorial.persons", p);
```
The first parameter to insert is the namespace. tutorial is the database and persons is the collection name.

#### getLastError

In order to ensure the write succeeded we need to call getLastError.

Get error result from the last operation on this connection:

```cpp
string mongo::DBClientWithCommands::getLastError(); // Empty string if no error
```

Get the full last error object:

```cpp
BSONObj DBClientWithCommands::getLastErrorDetailed();
```

For an example, see [this demo](https://github.com/mongodb/mongo-cxx-driver/blob/legacy/src/mongo/client/examples/simple_client_demo.cpp).

For additional background information on getLastError see the [write
operations documentation](https://www.mongodb.com/docs/manual/core/write-operations/#write-concern).

### Count

Let’s now fetch all objects from the persons collection, and display them. We’ll also show here how to use count().

```cpp
cout << "count:" << c.count("tutorial.persons") << endl;
```

### Query

```cpp
auto_ptr<DBClientCursor> cursor = c.query("tutorial.persons", BSONObj());

while (cursor->more())
   cout << cursor->next().toString() << endl;
```

`BSONObj()` is an empty BSON object – it represents `{}` which indicates an
empty query pattern (an empty query is a query for all objects).

We use `BSONObj::toString()` above to print out information about each
object retrieved. `BSONObj::toString` is a diagnostic function which prints
an abbreviated JSON string representation of the object. For full JSON
output, use `BSONObj::jsonString`.

Let’s now write a function which prints out the name (only) of all persons
in the collection whose age is a given value:

```cpp
void printIfAge(DBClientConnection& c, int age) {
    auto_ptr<DBClientCursor> cursor =
        c.query("tutorial.persons", MONGO_QUERY("age" << age));
    while (cursor->more()) {
        BSONObj p = cursor->next();
        cout << p.getStringField("name") << endl;
    }
}
```

`getStringField()` is a helper that assumes the name field is of type
string. To manipulate an element in a more generic fashion we can retrieve
the particular BSONElement from the enclosing object:

```cpp
BSONElement name = p["name"];
// or:
BSONElement name = p.getField("name");
```

See the api docs, and jsobj.h, for more information.

Our query above, written as JSON, is of the form

`{ age : <agevalue> }`

Queries are BSON objects of a particular format – in fact, we could have
used the BSON() macro above instead of MONGO_QUERY(). See class Query in
dbclient.h for more information on Query objects, and the Sorting section
below.

In the mongo shell (which uses javascript), we could invoke:

```js
use tutorial;
db.persons.find({age : 33});
```

### Indexing

Let’s suppose we want to have an index on age so that our queries are fast. We would use:

```cpp
c.createIndex("tutorial.persons", fromjson("{age:1}"));
```

In the above example we use a new function, fromjson. fromjson converts a
JSON string to a BSONObj. This is sometimes a convenient way to specify
BSON. Alternatively, we could have written:

```cpp
c.createIndex("tutorial.persons", BSON( "age" << 1 ));
```

While calling createIndex multiple times won't result in duplicate index
creation on the server, it will cause an extra network round-trip and
server operation for each call. It's best practice to use this method
sparingly, for example once at the beginning of your code, or perhaps in an
external setup script that configures the database for your application.
For more information about indexing, see the [MongoDB Indexing
docs](https://www.mongodb.com/docs/manual/indexes/).

### Sorting

Let’s now make the results from printIfAge sorted alphabetically by name.
To do this, we change the query statement from:

```cpp
auto_ptr<DBClientCursor> cursor = c.query("tutorial.persons", MONGO_QUERY("age" << age));
```

to

```cpp
auto_ptr<DBClientCursor> cursor = c.query("tutorial.persons", MONGO_QUERY("age" << age ).sort("name"));
```

Here we have used `Query::sort()` to add a modifier to our query expression for sorting.

### Updating

Use the `update()` method to perform a database update. For example the
following update in the mongo shell:

```js
> use tutorial
> db.persons.update(
    { name : 'Joe', age : 33 },
    { $inc : { visits : 1 } }
)
```

is equivalent to the following C++ code:

```cpp
db.update("tutorial.persons",
    BSON("name" << "Joe" << "age" << 33),
    BSON("$inc" << BSON( "visits" << 1))
);
```

The `update()` method can be used to modify specific fields, replace the
entire existing document, insert new documents or update multiple
documents.  In the MongoDB Manual, examples are provided in the [Modify
Documents Tutorial](https://www.mongodb.com/docs/manual/tutorial/modify-documents/).

### Arrays

A simple example illustrating usage of BSON arrays and the `$nin` operator
is available [here](https://github.com/mongodb/mongo-cxx-driver/blob/legacy/src/mongo/client/examples/arrayExample.cpp).

### Further Reading

This overview just touches on the basics of using MongoDB from C++. There
are many more capabilities. For further exploration:

 - See the language-independent [MongoDB Manual](https://www.mongodb.com/docs/manual/)
 - Experiment with the [mongo shell](https://www.mongodb.com/docs/mongodb-shell/)
 - Review the [API docs](https://mongodb.github.io/api/1.0.2)
