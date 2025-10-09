# Initial Commit 8c71d10

```
k@t+ release-build % ./src/Common/benchmarks/benchmark_hybrid_hash_table                                                                                                                                                                                       remotes/origin/enhance/fix-network-minor-issues~3 ✭ ◼
Unable to determine clock rate from sysctl: hw.cpufrequency: No such file or directory
This does not affect benchmark measurements, only the metadata output.
***WARNING*** Failed to set thread affinity. Estimated CPU frequency may be incorrect.
2024-10-24T11:39:39-07:00
Running ./src/Common/benchmarks/benchmark_hybrid_hash_table
Run on (12 X 24 MHz CPU s)
CPU Caches:
  L1 Data 64 KiB
  L1 Instruction 128 KiB
  L2 Unified 4096 KiB (x12)
Load Average: 43.11, 47.12, 28.12
Finished inserting i=10000000 keys
--------------------------------------------------------------------------------------------------------------------------
Benchmark                                                                                Time             CPU   Iterations
--------------------------------------------------------------------------------------------------------------------------
HybridTableFixture/UpsertBatch/1/100000/1/1/10/iterations:10000000                    1378 ns         1365 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/1/100/iterations:10000000                    971 ns          959 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/1/1000/iterations:10000000                   968 ns          957 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/1/10000/iterations:10000000                 1002 ns          991 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/1/100000/iterations:10000000                1022 ns         1013 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/0/10/iterations:10000000                    2135 ns         2110 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/0/100/iterations:10000000                   1702 ns         1680 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/0/1000/iterations:10000000                  1730 ns         1691 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/0/10000/iterations:10000000                 1713 ns         1700 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/0/100000/iterations:10000000                1909 ns         1893 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/Upsert/1/100000/1/1/iterations:10000000                            3281 ns         3224 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/Upsert/1/100000/0/0/iterations:10000000                            4179 ns         4110 ns     10000000
For each key found 10000000 keys
HybridTableFixture/ForEachKeyValue/1/100000/0/iterations:1                      2783103040 ns   2748797000 ns            1
Sequence found 100000 keys, iteration=100000
HybridTableFixture/ForKeyValueSequential/1/100000/0/10000000/iterations:100000        1189 ns         1182 ns       100000
Random found 100000 keys, iteration=100000
HybridTableFixture/ForKeyValueRandom/1/100000/1/10000000/iterations:100000            60.2 ns         60.0 ns       100000
Finished inserting i=300000000 keys
HybridTableFixture/UpsertBatch/2/100000/1/1/100/iterations:300000000                  1021 ns         1001 ns    300000000
Finished inserting i=300000000 keys
HybridTableFixture/UpsertBatch/2/100000/1/1/1000/iterations:300000000                 1005 ns          986 ns    300000000
Finished inserting i=300000000 keys
HybridTableFixture/UpsertBatch/2/100000/1/0/100/iterations:300000000                  1819 ns         1786 ns    300000000
Finished inserting i=300000000 keys
HybridTableFixture/UpsertBatch/2/100000/1/0/1000/iterations:300000000                 1785 ns         1750 ns    300000000
Finished inserting i=300000000 keys
HybridTableFixture/Upsert/2/100000/1/1/iterations:300000000                           3275 ns         3211 ns    300000000
Finished inserting i=300000000 keys
HybridTableFixture/Upsert/2/100000/0/0/iterations:300000000                           4248 ns         4164 ns    300000000
Sequence found 100000 keys, iteration=100000
HybridTableFixture/ForKeyValueSequential/2/100000/0/300000000/iterations:100000       1208 ns         1178 ns       100000
Random found 100000 keys, iteration=100000
HybridTableFixture/ForKeyValueRandom/2/100000/0/300000000/iterations:100000           59.9 ns         59.8 ns       100000
For each key found 300000000 keys
HybridTableFixture/ForEachKeyValue/2/100000/1/iterations:1                      8.0711e+10 ns   7.9308e+10 ns            1
```

# Some refinement, Commit 0cd6e47d

```
k@t+ release-build % ./src/Common/benchmarks/benchmark_hybrid_hash_table
Unable to determine clock rate from sysctl: hw.cpufrequency: No such file or directory
This does not affect benchmark measurements, only the metadata output.
***WARNING*** Failed to set thread affinity. Estimated CPU frequency may be incorrect.
2024-10-24T10:36:40-07:00
Running ./src/Common/benchmarks/benchmark_hybrid_hash_table
Run on (12 X 24 MHz CPU s)
CPU Caches:
  L1 Data 64 KiB
  L1 Instruction 128 KiB
  L2 Unified 4096 KiB (x12)
Load Average: 12.48, 36.34, 40.40
Finished inserting i=10000000 keys
--------------------------------------------------------------------------------------------------------------------------
Benchmark                                                                                Time             CPU   Iterations
--------------------------------------------------------------------------------------------------------------------------
HybridTableFixture/UpsertBatch/1/100000/1/1/10/iterations:10000000                     992 ns          965 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/1/100/iterations:10000000                    971 ns          954 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/1/1000/iterations:10000000                   970 ns          953 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/1/10000/iterations:10000000                  981 ns          964 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/1/100000/iterations:10000000                1051 ns         1029 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/0/10/iterations:10000000                    1777 ns         1739 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/0/100/iterations:10000000                   1714 ns         1682 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/0/1000/iterations:10000000                  1703 ns         1670 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/0/10000/iterations:10000000                 1847 ns         1791 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/0/100000/iterations:10000000                1970 ns         1911 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/Upsert/1/100000/1/1/iterations:10000000                            1013 ns          992 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/Upsert/1/100000/0/0/iterations:10000000                            1808 ns         1767 ns     10000000
For each key found 10000000 keys
HybridTableFixture/ForEachKeyValue/1/100000/0/iterations:1                      2679068042 ns   2653782000 ns            1
Sequence found 100000 keys, iteration=100000
HybridTableFixture/ForKeyValueSequential/1/100000/0/10000000/iterations:100000        1185 ns         1174 ns       100000
Random found 256 keys, iteration=100000
HybridTableFixture/ForKeyValueRandom/1/100000/1/10000000/iterations:100000             336 ns          326 ns       100000
Finished inserting i=300000000 keys
HybridTableFixture/UpsertBatch/2/100000/1/1/100/iterations:300000000                  1022 ns          998 ns    300000000
Finished inserting i=300000000 keys
HybridTableFixture/UpsertBatch/2/100000/1/1/1000/iterations:300000000                  997 ns          955 ns    300000000
Finished inserting i=300000000 keys
HybridTableFixture/UpsertBatch/2/100000/1/0/100/iterations:300000000                  1775 ns         1708 ns    300000000
Finished inserting i=300000000 keys
HybridTableFixture/UpsertBatch/2/100000/1/0/1000/iterations:300000000                 1776 ns         1721 ns    300000000
Finished inserting i=300000000 keys
HybridTableFixture/Upsert/2/100000/1/1/iterations:300000000                            988 ns          951 ns    300000000
Finished inserting i=300000000 keys
HybridTableFixture/Upsert/2/100000/0/0/iterations:300000000                           1831 ns         1775 ns    300000000
Sequence found 100000 keys, iteration=100000
HybridTableFixture/ForKeyValueSequential/2/100000/0/300000000/iterations:100000       1159 ns         1124 ns       100000
Random found 6910 keys, iteration=100000
HybridTableFixture/ForKeyValueRandom/2/100000/0/300000000/iterations:100000            747 ns          724 ns       100000
For each key found 300000000 keys
HybridTableFixture/ForEachKeyValue/2/100000/1/iterations:1                      7.9832e+10 ns   7.7803e+10 ns            1
```

# Commit b8318f99, std::span interface for emplaceNewKeys 
```
k@t+ release-build % ./src/Common/benchmarks/benchmark_hybrid_hash_table                                                                                                                                                                                                       ✘ 130 remotes/origin/develop ✭
Unable to determine clock rate from sysctl: hw.cpufrequency: No such file or directory
This does not affect benchmark measurements, only the metadata output.
***WARNING*** Failed to set thread affinity. Estimated CPU frequency may be incorrect.
2024-10-26T22:02:46-07:00
Running ./src/Common/benchmarks/benchmark_hybrid_hash_table
Run on (12 X 24 MHz CPU s)
CPU Caches:
  L1 Data 64 KiB
  L1 Instruction 128 KiB
  L2 Unified 4096 KiB (x12)
Load Average: 3.47, 4.76, 5.31
Finished inserting i=10000000 keys
--------------------------------------------------------------------------------------------------------------------------
Benchmark                                                                                Time             CPU   Iterations
--------------------------------------------------------------------------------------------------------------------------
HybridTableFixture/UpsertBatch/1/100000/1/1/10/iterations:10000000                    1802 ns         1768 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/1/100/iterations:10000000                   1698 ns         1671 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/1/1000/iterations:10000000                  1663 ns         1624 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/1/10000/iterations:10000000                 1747 ns         1724 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/1/100000/iterations:10000000                1914 ns         1881 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/0/10/iterations:10000000                    1781 ns         1745 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/0/100/iterations:10000000                   1749 ns         1712 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/0/1000/iterations:10000000                  1701 ns         1664 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/0/10000/iterations:10000000                 1747 ns         1708 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/0/100000/iterations:10000000                1835 ns         1803 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/Upsert/1/100000/1/1/iterations:10000000                            1016 ns          995 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/Upsert/1/100000/0/0/iterations:10000000                            1801 ns         1766 ns     10000000
For each key found 10000000 keys
HybridTableFixture/ForEachKeyValue/1/100000/0/iterations:1                      2738617709 ns   2702224000 ns            1
Sequence found 100000 keys, iteration=100000
HybridTableFixture/ForKeyValueSequential/1/100000/0/10000000/iterations:100000        1254 ns         1230 ns       100000
Random found 235 keys, iteration=100000
HybridTableFixture/ForKeyValueRandom/1/100000/1/10000000/iterations:100000             340 ns          329 ns       100000
Finished inserting i=300000000 keys
HybridTableFixture/UpsertBatch/2/100000/1/1/100/iterations:300000000                  1812 ns         1773 ns    300000000
Finished inserting i=300000000 keys
HybridTableFixture/UpsertBatch/2/100000/1/1/1000/iterations:300000000                 1810 ns         1775 ns    300000000
Finished inserting i=300000000 keys
HybridTableFixture/UpsertBatch/2/100000/1/0/100/iterations:300000000                  1806 ns         1773 ns    300000000
Finished inserting i=300000000 keys
HybridTableFixture/UpsertBatch/2/100000/1/0/1000/iterations:300000000                 1816 ns         1782 ns    300000000
Finished inserting i=300000000 keys
HybridTableFixture/Upsert/2/100000/1/1/iterations:300000000                            993 ns          973 ns    300000000
Finished inserting i=300000000 keys
HybridTableFixture/Upsert/2/100000/0/0/iterations:300000000                           1875 ns         1845 ns    300000000
Sequence found 100000 keys, iteration=100000
HybridTableFixture/ForKeyValueSequential/2/100000/0/300000000/iterations:100000       1187 ns         1160 ns       100000
Random found 6820 keys, iteration=100000
HybridTableFixture/ForKeyValueRandom/2/100000/0/300000000/iterations:100000            740 ns          736 ns       100000
For each key found 300000000 keys
HybridTableFixture/ForEachKeyValue/2/100000/1/iterations:1                      8.0624e+10 ns   7.9506e+10 ns            1
```

# Commit 617a8ccd, lazy init persistent hash table
```
k@t+ release-build % ./src/Common/benchmarks/benchmark_hybrid_hash_table                                                                                                                                                                                   chore/issue-6374-lazy-init-on-disk-hashtable ✭ ✱ ◼
Unable to determine clock rate from sysctl: hw.cpufrequency: No such file or directory
This does not affect benchmark measurements, only the metadata output.
***WARNING*** Failed to set thread affinity. Estimated CPU frequency may be incorrect.
2024-10-26T20:57:13-07:00
Running ./src/Common/benchmarks/benchmark_hybrid_hash_table
Run on (12 X 24 MHz CPU s)
CPU Caches:
L1 Data 64 KiB
L1 Instruction 128 KiB
L2 Unified 4096 KiB (x12)
Load Average: 3.12, 6.85, 8.63
Finished inserting i=10000000 keys
--------------------------------------------------------------------------------------------------------------------------
Benchmark                                                                                Time             CPU   Iterations
--------------------------------------------------------------------------------------------------------------------------
HybridTableFixture/UpsertBatch/1/100000/1/1/10/iterations:10000000                    1720 ns         1684 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/1/100/iterations:10000000                   1640 ns         1607 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/1/1000/iterations:10000000                  1650 ns         1614 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/1/10000/iterations:10000000                 1664 ns         1629 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/1/100000/iterations:10000000                1833 ns         1786 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/0/10/iterations:10000000                    1786 ns         1747 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/0/100/iterations:10000000                   1789 ns         1740 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/0/1000/iterations:10000000                  1830 ns         1771 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/0/10000/iterations:10000000                 1855 ns         1796 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/0/100000/iterations:10000000                1955 ns         1907 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/Upsert/1/100000/1/1/iterations:10000000                            1040 ns         1007 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/Upsert/1/100000/0/0/iterations:10000000                            1831 ns         1787 ns     10000000
For each key found 10000000 keys
HybridTableFixture/ForEachKeyValue/1/100000/0/iterations:1                      2874196458 ns   2835571000 ns            1
Sequence found 100000 keys, iteration=100000
HybridTableFixture/ForKeyValueSequential/1/100000/0/10000000/iterations:100000        1250 ns         1231 ns       100000
Random found 245 keys, iteration=100000
HybridTableFixture/ForKeyValueRandom/1/100000/1/10000000/iterations:100000             349 ns          346 ns       100000
Finished inserting i=300000000 keys
HybridTableFixture/UpsertBatch/2/100000/1/1/100/iterations:300000000                  1882 ns         1838 ns    300000000
Finished inserting i=300000000 keys
HybridTableFixture/UpsertBatch/2/100000/1/1/1000/iterations:300000000                 1914 ns         1866 ns    300000000
Finished inserting i=300000000 keys
HybridTableFixture/UpsertBatch/2/100000/1/0/100/iterations:300000000                  1807 ns         1775 ns    300000000
Finished inserting i=300000000 keys
HybridTableFixture/UpsertBatch/2/100000/1/0/1000/iterations:300000000                 1818 ns         1787 ns    300000000
Finished inserting i=300000000 keys
HybridTableFixture/Upsert/2/100000/1/1/iterations:300000000                           1002 ns          982 ns    300000000
Finished inserting i=300000000 keys
HybridTableFixture/Upsert/2/100000/0/0/iterations:300000000                           1881 ns         1847 ns    300000000
Sequence found 100000 keys, iteration=100000
HybridTableFixture/ForKeyValueSequential/2/100000/0/300000000/iterations:100000       1159 ns         1132 ns       100000
Random found 6890 keys, iteration=100000
HybridTableFixture/ForKeyValueRandom/2/100000/0/300000000/iterations:100000            746 ns          735 ns       100000
For each key found 300000000 keys
HybridTableFixture/ForEachKeyValue/2/100000/1/iterations:1                      8.0364e+10 ns   7.9739e+10 ns            1
```

# Commit 617a8ccd, lazy init persistent hash table and iterator interface
```
k@t+ release-build % ./src/Common/benchmarks/benchmark_hybrid_hash_table                                                                                                                                                                                     chore/issue-6374-lazy-init-on-disk-hashtable ✭ ✱
Unable to determine clock rate from sysctl: hw.cpufrequency: No such file or directory
This does not affect benchmark measurements, only the metadata output.
***WARNING*** Failed to set thread affinity. Estimated CPU frequency may be incorrect.
2024-10-26T23:25:53-07:00
Running ./src/Common/benchmarks/benchmark_hybrid_hash_table
Run on (12 X 24 MHz CPU s)
CPU Caches:
  L1 Data 64 KiB
  L1 Instruction 128 KiB
  L2 Unified 4096 KiB (x12)
Load Average: 11.60, 13.47, 11.83
Finished inserting i=10000000 keys
--------------------------------------------------------------------------------------------------------------------------
Benchmark                                                                                Time             CPU   Iterations
--------------------------------------------------------------------------------------------------------------------------
HybridTableFixture/UpsertBatch/1/100000/1/1/10/iterations:10000000                     967 ns          942 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/1/100/iterations:10000000                    940 ns          924 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/1/1000/iterations:10000000                   933 ns          918 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/1/10000/iterations:10000000                  970 ns          945 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/1/100000/iterations:10000000                1007 ns          983 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/0/10/iterations:10000000                    1796 ns         1765 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/0/100/iterations:10000000                   1739 ns         1708 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/0/1000/iterations:10000000                  1767 ns         1744 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/0/10000/iterations:10000000                 1724 ns         1697 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/0/100000/iterations:10000000                1908 ns         1871 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/Upsert/1/100000/1/1/iterations:10000000                             996 ns          975 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/Upsert/1/100000/0/0/iterations:10000000                            1796 ns         1760 ns     10000000
For each key found 10000000 keys
HybridTableFixture/ForEachKeyValue/1/100000/0/iterations:1                      2638080125 ns   2596742000 ns            1
Sequence found 100000 keys, iteration=100000
HybridTableFixture/ForKeyValueSequential/1/100000/0/10000000/iterations:100000        1182 ns         1158 ns       100000
Random found 248 keys, iteration=100000
HybridTableFixture/ForKeyValueRandom/1/100000/1/10000000/iterations:100000             328 ns          321 ns       100000
Finished inserting i=300000000 keys
HybridTableFixture/UpsertBatch/2/100000/1/1/100/iterations:300000000                  1012 ns          990 ns    300000000
Finished inserting i=300000000 keys
HybridTableFixture/UpsertBatch/2/100000/1/1/1000/iterations:300000000                 1003 ns          983 ns    300000000
Finished inserting i=300000000 keys
HybridTableFixture/UpsertBatch/2/100000/1/0/100/iterations:300000000                  1805 ns         1773 ns    300000000
Finished inserting i=300000000 keys
HybridTableFixture/UpsertBatch/2/100000/1/0/1000/iterations:300000000                 1810 ns         1777 ns    300000000
Finished inserting i=300000000 keys
HybridTableFixture/Upsert/2/100000/1/1/iterations:300000000                            999 ns          980 ns    300000000
Finished inserting i=300000000 keys
HybridTableFixture/Upsert/2/100000/0/0/iterations:300000000                           1882 ns         1847 ns    300000000
Sequence found 100000 keys, iteration=100000
HybridTableFixture/ForKeyValueSequential/2/100000/0/300000000/iterations:100000       1263 ns         1249 ns       100000
Random found 6875 keys, iteration=100000
HybridTableFixture/ForKeyValueRandom/2/100000/0/300000000/iterations:100000            754 ns          739 ns       100000
For each key found 300000000 keys
HybridTableFixture/ForEachKeyValue/2/100000/1/iterations:1                      7.9712e+10 ns   7.9175e+10 ns            1
```

# Commit 060c0912, tracking in memory changes for hybrid hash table
```
Running ./src/Common/benchmarks/benchmark_hybrid_hash_table
Run on (16 X 24 MHz CPU s)
CPU Caches:
  L1 Data 64 KiB
  L1 Instruction 128 KiB
  L2 Unified 4096 KiB (x16)
Load Average: 8.81, 5.30, 5.11
Finished inserting i=10000000 keys
--------------------------------------------------------------------------------------------------------------------------
Benchmark                                                                                Time             CPU   Iterations
--------------------------------------------------------------------------------------------------------------------------
HybridTableFixture/UpsertBatch/1/100000/1/1/10/iterations:10000000                    1307 ns         1304 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/1/100/iterations:10000000                   1211 ns         1210 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/1/1000/iterations:10000000                  1216 ns         1214 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/1/10000/iterations:10000000                 1174 ns         1172 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/1/100000/iterations:10000000                1268 ns         1267 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/0/10/iterations:10000000                    2010 ns         2009 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/0/100/iterations:10000000                   1853 ns         1852 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/0/1000/iterations:10000000                  1875 ns         1875 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/0/10000/iterations:10000000                 1857 ns         1855 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/UpsertBatch/1/100000/1/0/100000/iterations:10000000                1999 ns         1998 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/Upsert/1/100000/1/1/iterations:10000000                            1633 ns         1632 ns     10000000
Finished inserting i=10000000 keys
HybridTableFixture/Upsert/1/100000/0/0/iterations:10000000                            4283 ns         4262 ns     10000000
For each key found 10000000 keys
HybridTableFixture/ForEachKeyValue/1/100000/0/iterations:1                      2958173208 ns   2957060000 ns            1
Sequence found 100000 keys, iteration=100000
HybridTableFixture/ForKeyValueSequential/1/100000/0/10000000/iterations:100000        1276 ns         1275 ns       100000
Random found 100000 keys, iteration=100000
HybridTableFixture/ForKeyValueRandom/1/100000/1/10000000/iterations:100000            3882 ns         3881 ns       100000
Finished inserting i=300000000 keys
HybridTableFixture/UpsertBatch/2/100000/1/1/100/iterations:300000000                  1349 ns         1325 ns    300000000
Finished inserting i=300000000 keys
HybridTableFixture/UpsertBatch/2/100000/1/1/1000/iterations:300000000                 1371 ns         1330 ns    300000000
Finished inserting i=300000000 keys
HybridTableFixture/UpsertBatch/2/100000/1/0/100/iterations:300000000                  2064 ns         2043 ns    300000000
Finished inserting i=300000000 keys
HybridTableFixture/UpsertBatch/2/100000/1/0/1000/iterations:300000000                 2073 ns         2057 ns    300000000
Finished inserting i=300000000 keys
HybridTableFixture/Upsert/2/100000/1/1/100/iterations:300000000                       1986 ns         1920 ns    300000000
Finished inserting i=300000000 keys
HybridTableFixture/Upsert/2/100000/0/0/iterations:300000000                           4800 ns         4690 ns    300000000
Sequence found 100000 keys, iteration=100000
HybridTableFixture/ForKeyValueSequential/2/100000/0/300000000/iterations:100000       1350 ns         1350 ns       100000
Random found 100000 keys, iteration=100000
HybridTableFixture/ForKeyValueRandom/2/100000/0/300000000/iterations:100000           4835 ns         4834 ns       100000
For each key found 300000000 keys
HybridTableFixture/ForEachKeyValue/2/100000/0/iterations:1                      8.9276e+10 ns   8.8933e+10 ns            1
Random remove keys failed 0, iteration=100000
HybridTableFixture/RemoveKeysRandom/2/100000/0/100000/1000/iterations:100000           582 ns          577 ns       100000
Random remove keys failed 0, iteration=100000
HybridTableFixture/RemoveKeyRandom/2/100000/1/300000000/iterations:100000              800 ns          800 ns       100000
```
