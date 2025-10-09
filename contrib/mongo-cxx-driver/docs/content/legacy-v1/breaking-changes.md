+++
date = "2016-08-15T16:11:58+05:30"
title = "Breaking changes from 26compat"
[menu.main]
  weight = 25
  parent="legacy"
+++

The 26compat release series tracks the server 2.6 releases one-to-one. As a
result, it receives only bugfixes and small updates necessary to keep it
building in isolation.

The legacy release series, on the other hand, is a permanent and diverging
fork. Our philosophy is to keep the legacy branch as close to the 26compat
branch as is reasonable, but that when weighing new features against
compatibility, we will choose new features. As a result the legacy branch
is not 100% source compatible with the 26compat branch.

This page attempts to serve as a transition guide for those users looking
to migrate from the 26compat branch to the legacy branch. Note that it does
*not* discuss new features in detail and simply points to the per-release
notes.

# Breaking Changes

## Changes to the build system

Scons targets have been renamed to more 'obvious' names, and some unused or
unneeded targets have been removed.

### cheat sheet

Task                                  | Scons Target
--------------------------------------|-----------------
compile driver                        |`driver`
install driver                        |`install`
check driver install (used internally)|`check-install`
build unit tests                      |`build-unit`
run unit tests                        |`unit`
build integration tests               |`build-integration`
run integration tests                 |`integration`
build client examples                 |`build-examples`
run client examples                   |`examples`
build everything (driver, unit tests, integration tests, examples|`all`
run all tests and client examples     |`test`

### Details

* [`mongo-orchestration`](https://github.com/10gen/mongo-orchestration) is now required to run the driver's test suite. Please see the repository for instructions how to install and run `mongo-orchestration`. If you are not running tests, simply allow scons to time out when it looks for an instance of `mongo-orchestration` at the start of a build.
* The `driver` target has been created to built the client library without installing it
* The `install-mongoclient` target has been renamed to `install`
* Unit tests are now built with `build-unit`, and run with `unit`
* Integration tests are now built with `build-integration`, and run with `integration`
* Examples are now built with `build-examples`, and run with `examples`
* On OSX the `--osx-version-min` flag will now default to the current OSX version
* The `--full` flag is no longer required, and it is an error to specify it.
* The `--d` and `--dd` flags have been removed. Use the `--opt` and `--dbg` flags instead.
* The `--use-system-boost` flag is no longer required, and it is an error to specify it.
* All ABI affecting macros are now defined in a generated `config.h` header that is automatically included from `dbclient.h` and `bson.h`.
* Many server specific build options (that were unlikely to have been used when building the driver) have been removed.
* The default installation prefix is now `build/install`, rather than `/usr/local`.
* All build artifacts are now captured under the `build` directory.

## Changes to APIs
* The `mongo::be`, `mongo::bo`, and `mongo::bob` typedefs for `mongo::BSONElement`, `mongo::BSONObj` and `mongo::BSONObjBuilder` have been removed. We recommend using the fully qualified names in new code.
* The `mongo::BSONBuilderBase` class has been removed and is no longer a base class of `mongo::BSONObjBuilder` or `mongo::BSONArrayBuilder`
* The `OpTime` class has been completely removed. It has been replaced by the simplified `Timestamp_t` class.
* The `globalServerOptions` and `globalSSLOptions` objects and their classes have been removed. All driver configuration should be done through the new `mongo::client::Options` object.
* The `RamLog`, `RotatableFileAppender`, and `Console` classes have been removed from the logging subsystem.
* In addition, many auxiliary types, functions, and headers that were either unused, or minimally used, have been removed from the distribution.
* The `ensureIndex` and related methods have been removed. The replacement is the new `createIndex` method.
* `IndexSpec::dropDuplicates()` is now deprecated as it is a no-op in MongoDB 3.0.
* The `QUERY` macro has been replaced by `MONGO_QUERY`.
* The `ConnectionString::parse` method now requires it's argument to be in the MongoDB URL ("mongodb://...") format. To use the old format, use the new `ConnectionString::parseDeprecated` method.
* The `ConnectionPool` and `ScopedDbConnection` classes have been removed.

## Behavior Changes
* The driver will not function correctly unless `mongo::client::initialize` is invoked before using the driver APIs. The mongo::client::shutdown method should also be called at application termination (if options.callShutdownAtExit() is not set) so the driver can cleanly terminate. As a convenience, we have added [`mongo::GlobalInstance`](https://github.com/mongodb/mongo-cxx-driver/blob/legacy/src/mongo/client/init.h#L69) as an RAII wrapper to automatically call these methods.
* options.callShutdownAtExit() is a no-op on non-static builds on Windows due to issues around sudden thread termination.
* The driver no longer logs any output by default. You may configure and inject a logger to re-enable logging. See `src/mongo/client/examples/clientTest.cpp` for an example of how to enable logging.
* Writes are now "[acknowledged](https://www.mongodb.com/docs/manual/core/write-concern/#write-concern-acknowledged)" by default. In all previous releases the default write concern was “[unacknowledged](https://www.mongodb.com/docs/manual/core/write-concern/#unacknowledged)”. This change may have performance and behavior implications for existing applications that did not confirm writes. You can read more about the change [here](https://www.mongodb.com/docs/manual/release-notes/drivers-write-concern/#driver-write-concern-change).
* The driver now throws a mongo::OperationException when write concern is greater than Acknowledged for errors that occur when running operations against a MongoDB database.
* The default shutdown grace period is now zero which means the driver may block forever until a successful shutdown occurs.

# Improvements

Please see the [release
notes](https://github.com/mongodb/mongo-cxx-driver/releases) for the
individual legacy branch releases for details on improvements in each
release.
