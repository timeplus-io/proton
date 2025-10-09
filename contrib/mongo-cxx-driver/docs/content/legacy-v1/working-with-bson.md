+++
date = "2016-08-15T16:11:58+05:30"
title = "Working with BSON"
[menu.main]
  identifier = 'legacy-working-with-bson'
  weight = 15
  parent="legacy"
+++

###  BSON Helper Functions

This is a non-exhaustive list of helper functions for use in the C++ stream
syntax. An exhaustive list is here:
[bsonmisc.h](https://github.com/mongodb/mongo-cxx-driver/blob/legacy/src/mongo/bson/bsonmisc.h)

Typical example of stream syntax:

```cpp
BSONObj p = BSON( "name" << "Joe" << "age" << 33 );
```

#### GENOID
The server will add an ``_id`` automatically if it is not included explicitly.

```cpp
BSONObj p = BSON( GENOID << "name" << "Joe" << "age" << 33 );
// result is: { _id : ..., name : "Joe", age : 33 }
```

#### LT, GT, LTE, GTE, NE
less than, greater than, etc.

```cpp
BSONObj p = BSON( "age" << GT << 21 );
// result is:  { age : { $gt : 21 } }
```

#### DATENOW
Translates to current date

```cpp
BSONObj p = BSON( "created" << DATENOW );
// result is: { created : "2009-10-09 11:41:42" }
```

#### MINKEY, MAXKEY

```cpp
BSONObj p = BSON( "a" << MINKEY);
// result is: { "a" : { "$minKey" : 1 } }
```

#### OR

```cpp
OR(BSON("x" << GT << 7), BSON("y" << LT << 6))
// result is: {$or: [{x: {$gt: 7}}, {y: {$lt: 6}}]}
```

#### BSONNULL
Translates to ``null`` value (will appear in MongoDB 2.1)

```cpp
BSONObj p = BSON( "name" << "Methuselah" << "age" << BSONNULL );
// result is: { name : "Methuselah", age : null }
```
