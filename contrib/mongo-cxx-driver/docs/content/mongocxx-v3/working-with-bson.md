+++
date = "2016-08-15T16:11:58+05:30"
title = "Working with BSON"
[menu.main]
  identifier = 'mongocxx3-working-with-bson'
  weight = 15
  parent="mongocxx3"
+++

The mongocxx driver ships with a new library, bsoncxx.  This article will
go over some of the different types in this library, and how and when to
use each.  For more information and example code, see our
[examples](https://github.com/mongodb/mongo-cxx-driver/tree/master/examples/bsoncxx).

1. [Document Builders](#builders)<br/>
2. [Owning BSON Documents (values)](#value)<br/>
3. [Non-owning BSON Documents (views)](#view)<br/>
4. [Optionally-owning BSON Documents(view_or_value)](#view_or_value)<br/>
5. [BSON Document Lifetime](#lifetime)<br/>
6. [Printing BSON Documents](#print)<br/>
7. [Getting Fields out of BSON Documents](#fields)<br/>

## Document Builders {#builders}

The bsoncxx library offers four interfaces for building BSON: one-off
functions, a basic builder, a list builder and a stream-based builder.

[`bsoncxx::builder::basic::document`](https://github.com/mongodb/mongo-cxx-driver/blob/master/src/bsoncxx/include/bsoncxx/v_noabi/bsoncxx/builder/basic/document.hpp)<br/>
[`bsoncxx::builder::stream::document`](https://github.com/mongodb/mongo-cxx-driver/blob/master/src/bsoncxx/include/bsoncxx/v_noabi/bsoncxx/builder/stream/document.hpp)

The various methods of creating BSON documents and arrays are all
equivalent. All interfaces will provide the same results, the choice of
which to use is entirely aesthetic.

## List builder {#list}

The simplest way to create a BSON document or array is to use the JSON-like
list builder: 

```c++
// { "hello": "world" }
bsoncxx::builder::list list_builder = {"hello", "world"};
bsoncxx::document::view document = list_builder.view().get_document();
```

More advanced uses of the list builder are shown in [this
example](https://github.com/mongodb/mongo-cxx-driver/blob/master/examples/bsoncxx/builder_list.cpp).

## "One-off" builder functions {#one-off}

The "One-off" builder creates documents and arrays in a single call.
These can be used when no additional logic (such as conditionals or loops)
needs to be used to create the object:

```c++
using bsoncxx::builder::basic::kvp;

// { "hello": "world" }
bsoncxx::document::value document = bsoncxx::builder::basic::make_document(kvp("hello", "world"));
```

## Basic builder

```
using bsoncxx::builder::basic::kvp;

// { "hello" : "world" }
bsoncxx::builder::basic::document basic_builder{};
basic_builder.append(kvp("hello", "world"));
bsoncxx::document::value document = basic_builder.extract();
```

More advanced uses of the basic builder are shown in [this
example](https://github.com/mongodb/mongo-cxx-driver/blob/master/examples/bsoncxx/builder_basic.cpp).

## Stream builder {#stream-builder}

```
// { "hello" : "world" }

using bsoncxx::builder::stream;
bsoncxx::document::value document = stream::document{} << "hello" << "world" << stream::finalize;
```

More advanced uses of the stream builder are shown in [this
example](https://github.com/mongodb/mongo-cxx-driver/blob/master/examples/bsoncxx/builder_stream.cpp).

**NOTE**: In order to properly append each new value, a stream builder
needs to keep track of the state of the current document, including the
nesting level and the type of the most recent value appended to the
builder. The initial stream builder must *not* be reused after this state
changes, which means that intermediate values must be stored in new
variables if a document is being built with the stream builder across
multiple statements. Because doing this properly is difficult and the
compiler error messages can be confusing, using the stream builder is
discouraged. We recommend instead using the basic builder or the
[one-off builder functions](#one-off).

**Building arrays in loops**

Sometimes it's necessary to build an array using a loop. With the basic
builder, a top-level array can be built by simply calling `append` inside
a loop:

```
// [ 1, 2, 3 ]

const auto elements = {1, 2, 3};
auto array_builder = bsoncxx::builder::basic::array{};

for (const auto& element : elements) {
    array_builder.append(element);
}
```

To build a subarray in a loop, pass a lambda to `append` (or as the second
argument of `kvp` if the subarray is contained by a document rather than
an array):

```
// { "foo" : [ 1, 2, 3 ] }

using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::sub_array;

const auto elements = {1, 2, 3};
auto doc = bsoncxx::builder::basic::document{};
doc.append(kvp("foo", [&elements](sub_array child) {
    for (const auto& element : elements) {
        child.append(element);
    }
}));
```

When building an array with the stream builder, it's important to be aware
that the return type of using the `<<` operator on a stream builder is not
uniform. To build an array in a loop properly, intermediate values
returned by the stream builder should be stored in variables when the type
changes. One attempt to build an array from a stream builder using a loop
might look like the following:

```
// { "subdocs" : [ { "key" : 1 }, { "key" : 2 }, { "key" : 3 } ], "another_key" : 42 }
using namespace bsoncxx;

builder::stream::document builder{};

auto in_array = builder << "subdocs" << builder::stream::open_array;
for (auto&& e : {1, 2, 3}) {
    in_array = in_array << builder::stream::open_document << "key" << e
                        << builder::stream::close_document;
}
auto after_array = in_array << builder::stream::close_array;

after_array << "another_key" << 42;

document::value doc = after_array << builder::stream::finalize;

std::cout << to_json(doc) << std::endl;
```

Note that the result of any stream operation should be captured, so if you want
to split the single statement within the for loop above into multiple
statements, you must capture each intermediate result. Additionally, the last
statement within the loop body should assign its result back to the in_array
object, so that the loop restarts in a consistent state:

```
for (auto && e : {1, 2, 3}) {
    auto open_state = in_array << builder::stream::open_document;
    auto temp_state = open_state << "key" << e;
    in_array = temp_state << builder::stream::close_document;
}
```

### <a name="value">Owning BSON Documents (values)</a>

[`bsoncxx::document::value`](https://github.com/mongodb/mongo-cxx-driver/blob/master/src/bsoncxx/include/bsoncxx/v_noabi/bsoncxx/document/value.hpp)

This type represents an actual BSON document, one that owns its buffer of
data.  These documents can be constructed from a builder by calling
`extract()`:

```
bsoncxx::document::value basic_doc{basic_builder.extract()};
bsoncxx::document::value stream_doc{stream_builder.extract()};
```

Note that after calling `extract()` the builder is in a moved-from state,
and should not be used.

It's possible to create a `bsoncxx::document::value` in a single line using
the stream builder interface and the `finalize` token. `finalize` returns a
`document::value` from a temporary stream builder:

```
// { "finalize" : "is nifty" }
bsoncxx::document::value one_line = bsoncxx::builder::stream::document{} << "finalize" << "is nifty" << bsoncxx::builder::stream::finalize;
```

### <a name="view">Non-owning BSON Documents (views)</a>

[`bsoncxx::document::view`](https://github.com/mongodb/mongo-cxx-driver/blob/master/src/bsoncxx/include/bsoncxx/v_noabi/bsoncxx/document/view.hpp)

This type is a view into an owning `bsoncxx::document::value`.

```
bsoncxx::document::view document_view{document_value.view()};
```

A `document::value` also implicitly converts to a `document::view`:

```
bsoncxx::document::view document_view{document_value};
```

In performance-critical code, passing views around is preferable to using
values because we can avoid excess copying. Also, passing a view of a
document allows us to use the document multiple times:

```
// { "copies" : { "$gt" : 100 } }
auto query_value = document{} << "copies" << open_document << "$gt" << 100 << close_document << finalize;

// Run the same query across different collections
auto collection1 = db["science_fiction"];
auto cursor1 = collection1.find(query_value.view());

auto collection2 = db["cookbooks"];
auto cursor2 = collection2.find(query_value.view());
```

### <a name="view_or_value">Optionally-owning BSON Documents (view_or_value)</a>

Many driver methods take a document::view_or_value parameter, for example, [`run_command`](https://github.com/mongodb/mongo-cxx-driver/blob/master/src/mongocxx/include/mongocxx/v_noabi/mongocxx/database.hpp#L144-L153):

```
bsoncxx::document::value run_command(bsoncxx::document::view_or_value command);
```

Such methods can take either a `document::view` or a `document::value`. If
a `document::value` is passed in, it must be passed by r-value reference, so
ownership of the document is transferred to the method.

```
document::value ping = document{} << "ping" << 1 << finalize;

// You can pass a document::view into run_command()
db.run_command(ping.view());

// Or you can move in a document::value
db.run_command(std::move(ping));
```

You shouldn't need to create view_or_value types directly in order to use
the driver.  They are offered as a convenience method to allow driver
methods to take documents in either an owning or non-owning way.  The
`view_or_value` type also helps mitigate some of the lifetime issues
discussed in the next section.

### <a name="lifetime">BSON Document Lifetime</a>

It is imperative that `document::value`s outlive any `document::view`s that
use them. If the underlying value gets cleaned up, the view will be left
with a dangling pointer.  Consider a method that returns a view of a
newly-created document:

```
bsoncxx::document::view make_a_dangling_view() {
   bsoncxx::builder::basic::document builder{};
   builder.append(kvp("hello", "world"));

   // This creates a document::value on the stack that will disappear when we return.
   bsoncxx::document::value stack_value{builder.extract()};

   // We're returning a view of the local value
   return stack_value.view(); // Bad!!
}
```

This method returns a dangling view that should not be used:

```
// This view contains a dangling pointer
bsoncxx::document::view dangling_view = make_a_dangling_view(); // Warning!!
```

Attempting to create a view off of a builder will similarly create a
dangerous view object, because the temporary value returned from
`extract()` isn't captured:

```
bsoncxx::builder::stream::document temp_builder{};
temp_builder << "oh" << "no";
bsoncxx::document::view dangling_view = temp_builder.extract().view(); // Bad!!
```

### <a name="print">Printing BSON Documents</a>

[`bsoncxx::to_json()`](https://github.com/mongodb/mongo-cxx-driver/blob/master/src/bsoncxx/include/bsoncxx/v_noabi/bsoncxx/json.hpp#L39-L55)

The bsoncxx library comes with a convenience method to convert BSON
documents to strings for easy inspection:

```
bsoncxx::document::value = document{} << "I am" << "a BSON document" << finalize;
std::cout << bsoncxx::to_json(doc.view()) << std::endl;
```

There is an analogous method, [from_json()](https://github.com/mongodb/mongo-cxx-driver/blob/master/src/bsoncxx/include/bsoncxx/v_noabi/bsoncxx/json.hpp#L57-L67), to build document::values out of existing JSON strings.

### <a name="fields">Getting Fields out of BSON Documents</a>

The [ ] operator reaches into a BSON document to retrieve values:

```
// doc_view = { "store" : "Key Foods", "fruits" : [ "apple", "banana" ] }
auto store = doc_view["store"];
auto first_fruit = doc_view["fruits"][0];
```

This returns a `bsoncxx::document::element`, which holds the actual desired value:

```
document::element store_ele{doc_view["store"]};
if (store_ele) {
    // this block will only execute if "store" was found in the document
    std::cout << "Examining inventory at " << to_json(store_ele.get_value()) << std::endl;
}
```

This feature is shown in more detail in [this example](https://github.com/mongodb/mongo-cxx-driver/blob/master/examples/bsoncxx/getting_values.cpp) and [this example](https://github.com/mongodb/mongo-cxx-driver/blob/master/examples/bsoncxx/view_and_value.cpp).

### BSON Types

The [BSON specification](http://bsonspec.org/spec.html) provides a list
of supported types.  These are represented in C++ using the
[b_xxx](https://mongocxx.org/api/current/classes.html#letter_B)
type wrappers.

Some BSON types don't necessarily have a native representation to wrap and
are implemented via special classes.

#### Decimal128

The `bsoncxx::decimal128` class represents a 128-bit IEEE 754-2008 decimal
floating point value.  We expect users to convert these to and from
strings, but provide access to the low and high 64-bit values if users need
to convert to a native decimal128 type.

You can see how to work with `bsoncxx::decimal128` in [this example](https://github.com/mongodb/mongo-cxx-driver/blob/master/examples/bsoncxx/decimal128.cpp).
