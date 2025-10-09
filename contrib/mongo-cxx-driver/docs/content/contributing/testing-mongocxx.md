+++
date = "2016-08-15T16:11:58+05:30"
title = "Testing the mongocxx driver"
[menu.main]
  weight = 10
  parent="contributing"
+++

Tests for the C++11 driver use
[Catch](https://github.com/philsquared/Catch), a testing framework for C++.

Each class in the driver has a corresponding file in `src/mongocxx/test`.
Because the new driver wraps
[libmongoc](https://github.com/mongodb/mongo-c-driver), we prefer to mock
and test the behavior of individual classes rather than test end-to-end
behavior of operations against a running mongod.  In other words, these are
unit tests rather than integration tests.

We also have integration tests for this driver in `test/collection.cpp`.

## Running the existing tests

Build the tests with:

```
make
```

This will generate test binaries.  You can either run all the tests with:

```
make test
```

or, for more detailed output with Catch, run the generated binary:

```
./build/src/mongocxx/test/test_driver
```

or you can run the ctest command and make use of ctest's various flags. 
For example:

```
ctest -V
```

can be used to run the tests with verbose output, or 

```
ctest -R bson
```

could be used to run only the bson tests.

### Running integration tests

Some of the tests require a running mongod instance.  For this, first download
the [mongodb server](https://www.mongodb.com/download-center).

Then deploy a mongod on the default port with the command:

```
mongod --setParameter enableTestCommands=1
```

if it is installed, otherwise navigate to the directory holding the mongod
executable, and run:

```
./mongod --setParameter enableTestCommands=1
```

following either command with any flags you wish to use, excluding
`--port`.  While the mongod is running, run the tests as normal.

## Writing new tests

If you'd like to add a feature to the driver, please write a test for it as
well.  Additions to existing classes should have new sections added to the
existing test cases:

```
TEST_CASE("existing_class", "[existing_class]") {
   SECTION("Can do some new thing") {
      ...
      REQUIRE(new_thing_works);
   }
}
```

If you are adding a new class, please add a new test file for it to the
`test` directory.  The test file's name should match the new class's file's
name.  You will need to add your file as a source for the driver's test
target, in `src/mongocxx/test/CMakeLists.txt`:

```
set(mongocxx_test_sources
   ...
   some_new_class.cpp
   ...
)
```
