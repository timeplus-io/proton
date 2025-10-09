+++
date = "2016-08-15T16:11:58+05:30"
title = "Testing the legacy driver"
[menu.main]
  weight = 100
  parent="contributing"
+++

If you contribute to the C++ driver, you'll need to test your changes.  The
driver comes with a number of tests to ensure its functionality and
performance.  There are a few different kinds of tests within the driver's
codebase.

Note: if you are running OS X Mavericks or above, you may need to include the ```--osx-version-min=10.9``` flag to the commands below.

Note: The 26compat branch differs from the instructions below as follows:
* MongoOrchestration is not required.
* The target to run unit tests is 'smokeCppUnittests' (or 'test', or 'smoke'), not 'unit'
* The build-\[test] aliases are not supported (the run- aliases are supported, however)
* There are no integration tests
* The target to run the examples is 'smokeClient', not 'examples'
* You must have a mongod running on port 27999 to run the examples.
* The 'test' target does not run all tests, only the unit tests (see above).

### Unit tests

Unit tests do not require a running mongod or mongo-orchestration. These
tests are designed to test individual components of the driver in
isolation, and the test files are found in the same directory as the things
they test (so, ```bson_validate.cpp``` and ```bson_validate_test.cpp``` are
both found in ```src/mongo/bson```).  The different unit tests are listed
[here](https://github.com/mongodb/mongo-cxx-driver/blob/e240e0604678b1028aaee63e8de98e18047f7f31/src/mongo/SConscript#L49).

Build all the unit tests with scons:

```
> scons build-unit
```

Build and run all the unit tests:

```
> scons unit
```

Build an individual unit test:

```
> scons build-full/test/name
```

Build and run an individual unit test:

```
> scons run-full/test/name
```

### Integration Tests

Integration tests must run against [Mongo
Orchestration](https://github.com/10gen/mongo-orchestration).  Install and
setup Mongo Orchestration as follows:

```
> git clone https://github.com/10gen/mongo-orchestration.git
> cd mongo-orchestration
> python setup.py install
```

To run Mongo Orchestration, you'll need to alter the provided config file
to fit your system.  Open ```mongo-orchestration.config``` and replace the
paths there with paths to your MongoDB binaries.  It is only required to
have one MongoDB version defined as well as a last_updated field with a
date, so feel free to keep one of the ```"releases"``` and delete the other
entries. Mongo orchestration is **very** strict about JSON so no trailing
commas please.

Start up Mongo Orchestration and leave it running in the background while
you run the integration test suite:

```
> mongo-orchestration -f mongo-orchestration.config -e <release_name> start
```

The integration tests are located in ```src/mongo/integration```.
Additionally, some tests require the parameter ```enableTestCommands``` to
be set. There is a list of the different integration tests
[here](https://github.com/mongodb/mongo-cxx-driver/blob/e240e0604678b1028aaee63e8de98e18047f7f31/src/mongo/SConscript#L114).

To build all the integration tests:

```
> scons build-integration
```

Build and run all the integration tests:

```
> scons integration
```

Individual integration tests can be run in the same way as individual unit tests, shown above.

Note: to run the SASL integration tests, you should build with the ```--use-sasl-client``` flag.

### Client Example Programs

The driver includes a number of example programs of its use.  The examples
are listed
[here](https://github.com/mongodb/mongo-cxx-driver/blob/e240e0604678b1028aaee63e8de98e18047f7f31/src/SConscript.client#L189),
and the source files are found in ```src/mongo/client/examples```.  The
examples expect a mongod to be running locally on port 27999.

Build the examples with scons:

```
> scons build-examples
```

Build and run the examples:

```
> scons examples
```

### Run all tests

Run the unit tests, integration tests, and examples with scons:

```
> scons test
```
