+++
date = "2016-08-15T16:11:58+05:30"
title = "The mongocxx (v3) driver"
[menu.main]
  identifier = 'mongocxx3-overview'
  weight = 0
  parent="mongocxx3"
+++

The mongocxx is a ground-up rewrite of a C++ driver for MongoDB based on
[libmongoc](https://mongoc.org/).  It requires a C++11 compiler.  It is
known to build on x86 and x86-64 architectures for Linux, macOS,
Windows, and FreeBSD.

The mongocxx driver library includes a matching bson package, bsoncxx, that
implements the BSON specification (see http://www.bsonspec.org). This
library can be used standalone for object serialization and deserialization
even when one is not using MongoDB at all.

Releases of the mongocxx driver have version numbers like v3.x.y.

(Note: there were no v2.x.y C++ drivers to avoid confusion with the
deprecated legacy-0.0-26compat-2.x.y drivers.)

