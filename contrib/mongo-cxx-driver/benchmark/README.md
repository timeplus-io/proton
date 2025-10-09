This suite implements the benchmarks described in this spec: https://docs.google.com/document/d/1x7dCHHx_owOFYIPZ6VQwPerSWRhdkMSiepyFrNSFlLw/edit#

# Compiling
Configure the C++ driver to build with C++17 or newer and build the `microbenchmarks` target.

# Running
In order to run the microbenchmarks, first run `etc/microbenchmark-test-data.sh` to download the test data.

In order to run specific tests, just specify their names as arguments:
`build/benchmark/microbenchmarks BSONBench MultiBench`

To run all tests, specify `all` as an argument:
`build/benchmark/microbenchmarks all`

Full list of options:
BSONBench
SingleBench
ParallelBench
ReadBench
WriteBench
RunCommandBench

Note: run both the download script and the microbenchmarks binary from the project root.

# Notes
Note that in order to compare against the other drivers, an inMemory mongod instance should be 
used.

At this point, bson_decoding has not been added to the benchmarking suite due to the fact that
extended_bson has not been added to the C++ driver (CXX-1241).

Also note that the BSONBench tests are implemented to mirror the C driver's interpretation of the spec.