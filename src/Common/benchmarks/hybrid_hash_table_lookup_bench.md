# Lookup benchmark

Looks like hash index doesn't have perf benefit for this key distribution : std::uniform_int_distribution 

```
k@t+ release-build % ./src/Common/benchmarks/benchmark_hybrid_hash_table_lookup
Unable to determine clock rate from sysctl: hw.cpufrequency: No such file or directory
This does not affect benchmark measurements, only the metadata output.
***WARNING*** Failed to set thread affinity. Estimated CPU frequency may be incorrect.
2024-12-09T16:39:35-08:00
Running ./src/Common/benchmarks/benchmark_hybrid_hash_table_lookup
Run on (12 X 24 MHz CPU s)
CPU Caches:
  L1 Data 64 KiB
  L1 Instruction 128 KiB
  L2 Unified 4096 KiB (x12)
Load Average: 7.71, 7.85, 6.80
Finished inserting i=300000000 keys
Random found 100000 keys, iteration=100000
----------------------------------------------------------------------------------------------------------------------------
Benchmark                                                                                  Time             CPU   Iterations
----------------------------------------------------------------------------------------------------------------------------
HybridTableFixture/KeyLookupRandom/1/0/0/1/300000000/iterations:100000                  9744 ns         9496 ns       100000
Finished inserting i=300000000 keys
Random found 100000 keys, iteration=100000
HybridTableFixture/KeyLookupRandom/1/10000/0/1/300000000/iterations:100000             10773 ns        10736 ns       100000
Finished inserting i=300000000 keys
Random found 5050000 keys, iteration=100000
HybridTableFixture/KeyLookupRandomBatch/1/0/0/1/300000000/1000/iterations:100        6390439 ns      6346170 ns          100
Random found 5050000 keys, iteration=100000
HybridTableFixture/KeyLookupRandomBatch/1/10000/1/1/300000000/1000/iterations:100    6638242 ns      6586310 ns          100
Finished inserting i=300000000 keys
Random found 100000 keys, iteration=100000
HybridTableFixture/KeyLookupRandom/2/0/0/0/300000000/iterations:100000                  9714 ns         9503 ns       100000
Finished inserting i=300000000 keys
Random found 100000 keys, iteration=100000
HybridTableFixture/KeyLookupRandom/2/10000/0/0/300000000/iterations:100000             10916 ns        10650 ns       100000
Finished inserting i=300000000 keys
Random found 5050000 keys, iteration=100000
HybridTableFixture/KeyLookupRandomBatch/2/0/0/0/300000000/1000/iterations:100        7730098 ns      7442740 ns          100
Random found 5050000 keys, iteration=100000
HybridTableFixture/KeyLookupRandomBatch/2/10000/1/0/300000000/1000/iterations:100    6946621 ns      6842410 ns          100
```

# Commit 060c0912, tracking in memory changes for hybrid hash table
```
Running ./src/Common/benchmarks/benchmark_hybrid_hash_table_lookup
Run on (16 X 24 MHz CPU s)
CPU Caches:
  L1 Data 64 KiB
  L1 Instruction 128 KiB
  L2 Unified 4096 KiB (x16)
Load Average: 5.27, 3.75, 3.63
Finished inserting i=300000000 keys
Random found 100000 keys, iteration=100000
----------------------------------------------------------------------------------------------------------------------------
Benchmark                                                                                  Time             CPU   Iterations
----------------------------------------------------------------------------------------------------------------------------
HybridTableFixture/KeyLookupRandom/1/0/0/1/300000000/iterations:100000                  5204 ns         5203 ns       100000
Random found 100000 keys, iteration=100000
HybridTableFixture/KeyLookupRandom/1/10000/0/1/300000000/iterations:100000              5365 ns         5363 ns       100000
Random found 5050000 keys, iteration=100000
HybridTableFixture/KeyLookupRandomBatch/1/0/0/1/300000000/1000/iterations:100        6328744 ns      6328240 ns          100
Random found 5050000 keys, iteration=100000
HybridTableFixture/KeyLookupRandomBatch/1/10000/1/1/300000000/1000/iterations:100    6388503 ns      6388240 ns          100
Finished inserting i=300000000 keys
Random found 100000 keys, iteration=100000
HybridTableFixture/KeyLookupRandom/2/0/0/0/300000000/iterations:100000                  4746 ns         4744 ns       100000
Random found 100000 keys, iteration=100000
HybridTableFixture/KeyLookupRandom/2/10000/0/0/300000000/iterations:100000              5080 ns         5079 ns       100000
Random found 5050000 keys, iteration=100000
HybridTableFixture/KeyLookupRandomBatch/2/0/0/0/300000000/1000/iterations:100        6060041 ns      6059470 ns          100
Random found 5050000 keys, iteration=100000
HybridTableFixture/KeyLookupRandomBatch/2/10000/1/0/300000000/1000/iterations:100    5988360 ns      5987760 ns          100
```
