## new

```
(base) ➜  build_relwith_deb git:(feat/issue-7950-bump-up-v8) ✗ ./src/V8/benchmarks/benchmark_v8_perf 
2025-11-04T14:28:55+00:00
Running ./src/V8/benchmarks/benchmark_v8_perf
Run on (32 X 2695.95 MHz CPU s)
CPU Caches:
  L1 Data 48 KiB (x16)
  L1 Instruction 32 KiB (x16)
  L2 Unified 1024 KiB (x16)
  L3 Unified 32768 KiB (x2)
Load Average: 0.70, 0.96, 1.45
------------------------------------------------------------------------------------------------------------------------------------------
Benchmark                                                                                Time             CPU   Iterations UserCounters...
------------------------------------------------------------------------------------------------------------------------------------------
V8PerfFixture/BM_V8_Recompile/8/64/process_time/real_time                             2225 us         2400 us          308 ProcessRepeats=3 UDAObjects=8 ValuesSize=64
V8PerfFixture/BM_V8_Recompile/16/64/process_time/real_time                            4151 us         4387 us          176 ProcessRepeats=3 UDAObjects=16 ValuesSize=64
V8PerfFixture/BM_V8_Recompile/64/64/process_time/real_time                           17900 us        26581 us           39 ProcessRepeats=3 UDAObjects=64 ValuesSize=64
V8PerfFixture/BM_V8_Recompile/256/64/process_time/real_time                          81091 us        93746 us           11 ProcessRepeats=3 UDAObjects=256 ValuesSize=64
V8PerfFixture/BM_V8_Recompile/1024/64/process_time/real_time                        287658 us       382627 us            3 ProcessRepeats=3 UDAObjects=1.024k ValuesSize=64
V8PerfFixture/BM_V8_Recompile/2048/64/process_time/real_time                        585897 us       845666 us            1 ProcessRepeats=3 UDAObjects=2.048k ValuesSize=64
V8PerfFixture/BM_V8_Recompile/8/256/process_time/real_time                            2812 us         3590 us          262 ProcessRepeats=3 UDAObjects=8 ValuesSize=256
V8PerfFixture/BM_V8_Recompile/16/256/process_time/real_time                           5142 us         6468 us          142 ProcessRepeats=3 UDAObjects=16 ValuesSize=256
V8PerfFixture/BM_V8_Recompile/64/256/process_time/real_time                          22945 us        40483 us           32 ProcessRepeats=3 UDAObjects=64 ValuesSize=256
V8PerfFixture/BM_V8_Recompile/256/256/process_time/real_time                         93661 us       120721 us            9 ProcessRepeats=3 UDAObjects=256 ValuesSize=256
V8PerfFixture/BM_V8_Recompile/1024/256/process_time/real_time                       349380 us       586184 us            2 ProcessRepeats=3 UDAObjects=1.024k ValuesSize=256
V8PerfFixture/BM_V8_Recompile/2048/256/process_time/real_time                       690298 us      1072427 us            1 ProcessRepeats=3 UDAObjects=2.048k ValuesSize=256
V8PerfFixture/BM_V8_Recompile/8/1024/process_time/real_time                           4305 us         7370 us          152 ProcessRepeats=3 UDAObjects=8 ValuesSize=1.024k
V8PerfFixture/BM_V8_Recompile/16/1024/process_time/real_time                          7539 us        12660 us           92 ProcessRepeats=3 UDAObjects=16 ValuesSize=1.024k
V8PerfFixture/BM_V8_Recompile/64/1024/process_time/real_time                         29631 us        54021 us           23 ProcessRepeats=3 UDAObjects=64 ValuesSize=1.024k
V8PerfFixture/BM_V8_Recompile/256/1024/process_time/real_time                       122365 us       179001 us            7 ProcessRepeats=3 UDAObjects=256 ValuesSize=1.024k
V8PerfFixture/BM_V8_Recompile/1024/1024/process_time/real_time                      463651 us       780477 us            2 ProcessRepeats=3 UDAObjects=1.024k ValuesSize=1.024k
V8PerfFixture/BM_V8_Recompile/2048/1024/process_time/real_time                      949858 us      1519745 us            1 ProcessRepeats=3 UDAObjects=2.048k ValuesSize=1.024k
V8PerfFixture/BM_V8_Recompile/8/2048/process_time/real_time                           6132 us        13195 us          110 ProcessRepeats=3 UDAObjects=8 ValuesSize=2.048k
V8PerfFixture/BM_V8_Recompile/16/2048/process_time/real_time                         10487 us        22823 us           66 ProcessRepeats=3 UDAObjects=16 ValuesSize=2.048k
V8PerfFixture/BM_V8_Recompile/64/2048/process_time/real_time                         41863 us       100279 us           16 ProcessRepeats=3 UDAObjects=64 ValuesSize=2.048k
V8PerfFixture/BM_V8_Recompile/256/2048/process_time/real_time                       158434 us       334087 us            5 ProcessRepeats=3 UDAObjects=256 ValuesSize=2.048k
V8PerfFixture/BM_V8_Recompile/1024/2048/process_time/real_time                      647455 us      1614707 us            1 ProcessRepeats=3 UDAObjects=1.024k ValuesSize=2.048k
V8PerfFixture/BM_V8_Recompile/2048/2048/process_time/real_time                     1292041 us      2992641 us            1 ProcessRepeats=3 UDAObjects=2.048k ValuesSize=2.048k
V8PerfFixture/BM_V8_CompileOnce_Create/8/64/process_time/real_time                     893 us         1262 us          787 ProcessRepeats=3 UDAObjects=8 ValuesSize=64
V8PerfFixture/BM_V8_CompileOnce_Create/16/64/process_time/real_time                    929 us         1274 us          800 ProcessRepeats=3 UDAObjects=16 ValuesSize=64
V8PerfFixture/BM_V8_CompileOnce_Create/64/64/process_time/real_time                   1456 us         1836 us          505 ProcessRepeats=3 UDAObjects=64 ValuesSize=64
V8PerfFixture/BM_V8_CompileOnce_Create/256/64/process_time/real_time                  3629 us         4590 us          192 ProcessRepeats=3 UDAObjects=256 ValuesSize=64
V8PerfFixture/BM_V8_CompileOnce_Create/1024/64/process_time/real_time                11497 us        12708 us           61 ProcessRepeats=3 UDAObjects=1.024k ValuesSize=64
V8PerfFixture/BM_V8_CompileOnce_Create/2048/64/process_time/real_time                22271 us        23772 us           31 ProcessRepeats=3 UDAObjects=2.048k ValuesSize=64
V8PerfFixture/BM_V8_CompileOnce_Create/8/256/process_time/real_time                   1083 us         1483 us          599 ProcessRepeats=3 UDAObjects=8 ValuesSize=256
V8PerfFixture/BM_V8_CompileOnce_Create/16/256/process_time/real_time                  1410 us         1854 us          471 ProcessRepeats=3 UDAObjects=16 ValuesSize=256
V8PerfFixture/BM_V8_CompileOnce_Create/64/256/process_time/real_time                  3177 us         4307 us          214 ProcessRepeats=3 UDAObjects=64 ValuesSize=256
V8PerfFixture/BM_V8_CompileOnce_Create/256/256/process_time/real_time                10384 us        11660 us           70 ProcessRepeats=3 UDAObjects=256 ValuesSize=256
V8PerfFixture/BM_V8_CompileOnce_Create/1024/256/process_time/real_time               36309 us        38118 us           19 ProcessRepeats=3 UDAObjects=1.024k ValuesSize=256
V8PerfFixture/BM_V8_CompileOnce_Create/2048/256/process_time/real_time               70710 us        72993 us           10 ProcessRepeats=3 UDAObjects=2.048k ValuesSize=256
V8PerfFixture/BM_V8_CompileOnce_Create/8/1024/process_time/real_time                  2030 us         3338 us          348 ProcessRepeats=3 UDAObjects=8 ValuesSize=1.024k
V8PerfFixture/BM_V8_CompileOnce_Create/16/1024/process_time/real_time                 3041 us         4570 us          225 ProcessRepeats=3 UDAObjects=16 ValuesSize=1.024k
V8PerfFixture/BM_V8_CompileOnce_Create/64/1024/process_time/real_time                 9843 us        11681 us           69 ProcessRepeats=3 UDAObjects=64 ValuesSize=1.024k
V8PerfFixture/BM_V8_CompileOnce_Create/256/1024/process_time/real_time               33874 us        36089 us           19 ProcessRepeats=3 UDAObjects=256 ValuesSize=1.024k
V8PerfFixture/BM_V8_CompileOnce_Create/1024/1024/process_time/real_time             137343 us       140818 us            5 ProcessRepeats=3 UDAObjects=1.024k ValuesSize=1.024k
V8PerfFixture/BM_V8_CompileOnce_Create/2048/1024/process_time/real_time             266604 us       269928 us            3 ProcessRepeats=3 UDAObjects=2.048k ValuesSize=1.024k
V8PerfFixture/BM_V8_CompileOnce_Create/8/2048/process_time/real_time                  3223 us         4960 us          220 ProcessRepeats=3 UDAObjects=8 ValuesSize=2.048k
V8PerfFixture/BM_V8_CompileOnce_Create/16/2048/process_time/real_time                 5402 us         7222 us          126 ProcessRepeats=3 UDAObjects=16 ValuesSize=2.048k
V8PerfFixture/BM_V8_CompileOnce_Create/64/2048/process_time/real_time                17638 us        19658 us           40 ProcessRepeats=3 UDAObjects=64 ValuesSize=2.048k
V8PerfFixture/BM_V8_CompileOnce_Create/256/2048/process_time/real_time               69976 us        72931 us           11 ProcessRepeats=3 UDAObjects=256 ValuesSize=2.048k
V8PerfFixture/BM_V8_CompileOnce_Create/1024/2048/process_time/real_time             263753 us       267564 us            3 ProcessRepeats=3 UDAObjects=1.024k ValuesSize=2.048k
V8PerfFixture/BM_V8_CompileOnce_Create/2048/2048/process_time/real_time             520532 us       523738 us            1 ProcessRepeats=3 UDAObjects=2.048k ValuesSize=2.048k
V8PerfFixture/BM_V8_CompileOnce_Clone/8/64/process_time/real_time                      909 us         1279 us          786 ProcessRepeats=3 UDAObjects=8 ValuesSize=64
V8PerfFixture/BM_V8_CompileOnce_Clone/16/64/process_time/real_time                     896 us         1230 us          786 ProcessRepeats=3 UDAObjects=16 ValuesSize=64
V8PerfFixture/BM_V8_CompileOnce_Clone/64/64/process_time/real_time                    1520 us         1922 us          473 ProcessRepeats=3 UDAObjects=64 ValuesSize=64
V8PerfFixture/BM_V8_CompileOnce_Clone/256/64/process_time/real_time                   3669 us         4687 us          191 ProcessRepeats=3 UDAObjects=256 ValuesSize=64
V8PerfFixture/BM_V8_CompileOnce_Clone/1024/64/process_time/real_time                 12116 us        13505 us           58 ProcessRepeats=3 UDAObjects=1.024k ValuesSize=64
V8PerfFixture/BM_V8_CompileOnce_Clone/2048/64/process_time/real_time                 22810 us        24252 us           29 ProcessRepeats=3 UDAObjects=2.048k ValuesSize=64
V8PerfFixture/BM_V8_CompileOnce_Clone/8/256/process_time/real_time                    1104 us         1533 us          645 ProcessRepeats=3 UDAObjects=8 ValuesSize=256
V8PerfFixture/BM_V8_CompileOnce_Clone/16/256/process_time/real_time                   1375 us         1816 us          537 ProcessRepeats=3 UDAObjects=16 ValuesSize=256
V8PerfFixture/BM_V8_CompileOnce_Clone/64/256/process_time/real_time                   3221 us         4378 us          228 ProcessRepeats=3 UDAObjects=64 ValuesSize=256
V8PerfFixture/BM_V8_CompileOnce_Clone/256/256/process_time/real_time                  9991 us        11268 us           67 ProcessRepeats=3 UDAObjects=256 ValuesSize=256
V8PerfFixture/BM_V8_CompileOnce_Clone/1024/256/process_time/real_time                36155 us        38040 us           19 ProcessRepeats=3 UDAObjects=1.024k ValuesSize=256
V8PerfFixture/BM_V8_CompileOnce_Clone/2048/256/process_time/real_time                69363 us        71624 us           11 ProcessRepeats=3 UDAObjects=2.048k ValuesSize=256
V8PerfFixture/BM_V8_CompileOnce_Clone/8/1024/process_time/real_time                   1982 us         3219 us          356 ProcessRepeats=3 UDAObjects=8 ValuesSize=1.024k
V8PerfFixture/BM_V8_CompileOnce_Clone/16/1024/process_time/real_time                  3069 us         4613 us          214 ProcessRepeats=3 UDAObjects=16 ValuesSize=1.024k
V8PerfFixture/BM_V8_CompileOnce_Clone/64/1024/process_time/real_time                  9830 us        11578 us           69 ProcessRepeats=3 UDAObjects=64 ValuesSize=1.024k
V8PerfFixture/BM_V8_CompileOnce_Clone/256/1024/process_time/real_time                34792 us        37041 us           20 ProcessRepeats=3 UDAObjects=256 ValuesSize=1.024k
V8PerfFixture/BM_V8_CompileOnce_Clone/1024/1024/process_time/real_time              132903 us       136504 us            5 ProcessRepeats=3 UDAObjects=1.024k ValuesSize=1.024k
V8PerfFixture/BM_V8_CompileOnce_Clone/2048/1024/process_time/real_time              268460 us       272087 us            3 ProcessRepeats=3 UDAObjects=2.048k ValuesSize=1.024k
V8PerfFixture/BM_V8_CompileOnce_Clone/8/2048/process_time/real_time                   3205 us         4962 us          223 ProcessRepeats=3 UDAObjects=8 ValuesSize=2.048k
V8PerfFixture/BM_V8_CompileOnce_Clone/16/2048/process_time/real_time                  5364 us         7150 us          131 ProcessRepeats=3 UDAObjects=16 ValuesSize=2.048k
V8PerfFixture/BM_V8_CompileOnce_Clone/64/2048/process_time/real_time                 17995 us        19982 us           38 ProcessRepeats=3 UDAObjects=64 ValuesSize=2.048k
V8PerfFixture/BM_V8_CompileOnce_Clone/256/2048/process_time/real_time                68277 us        71172 us           10 ProcessRepeats=3 UDAObjects=256 ValuesSize=2.048k
V8PerfFixture/BM_V8_CompileOnce_Clone/1024/2048/process_time/real_time              269130 us       273006 us            2 ProcessRepeats=3 UDAObjects=1.024k ValuesSize=2.048k
V8PerfFixture/BM_V8_CompileOnce_Clone/2048/2048/process_time/real_time              518907 us       522282 us            1 ProcessRepeats=3 UDAObjects=2.048k ValuesSize=2.048k
V8PerfFixture/BM_V8_Recompile_Mutable/8/64/process_time/real_time                     2271 us         2446 us          299 ProcessRepeats=3 UDAObjects=8 ValuesSize=64
V8PerfFixture/BM_V8_Recompile_Mutable/16/64/process_time/real_time                    4149 us         4352 us          170 ProcessRepeats=3 UDAObjects=16 ValuesSize=64
V8PerfFixture/BM_V8_Recompile_Mutable/64/64/process_time/real_time                   18566 us        28449 us           41 ProcessRepeats=3 UDAObjects=64 ValuesSize=64
V8PerfFixture/BM_V8_Recompile_Mutable/256/64/process_time/real_time                  82641 us        96531 us           11 ProcessRepeats=3 UDAObjects=256 ValuesSize=64
V8PerfFixture/BM_V8_Recompile_Mutable/1024/64/process_time/real_time                292385 us       388904 us            3 ProcessRepeats=3 UDAObjects=1.024k ValuesSize=64
V8PerfFixture/BM_V8_Recompile_Mutable/2048/64/process_time/real_time                598619 us       859965 us            1 ProcessRepeats=3 UDAObjects=2.048k ValuesSize=64
V8PerfFixture/BM_V8_Recompile_Mutable/8/256/process_time/real_time                    2784 us         3568 us          242 ProcessRepeats=3 UDAObjects=8 ValuesSize=256
V8PerfFixture/BM_V8_Recompile_Mutable/16/256/process_time/real_time                   5046 us         6315 us          100 ProcessRepeats=3 UDAObjects=16 ValuesSize=256
V8PerfFixture/BM_V8_Recompile_Mutable/64/256/process_time/real_time                  22879 us        39661 us           30 ProcessRepeats=3 UDAObjects=64 ValuesSize=256
V8PerfFixture/BM_V8_Recompile_Mutable/256/256/process_time/real_time                 91899 us       118597 us            9 ProcessRepeats=3 UDAObjects=256 ValuesSize=256
V8PerfFixture/BM_V8_Recompile_Mutable/1024/256/process_time/real_time               347842 us       497775 us            2 ProcessRepeats=3 UDAObjects=1.024k ValuesSize=256
V8PerfFixture/BM_V8_Recompile_Mutable/2048/256/process_time/real_time               696635 us      1052663 us            1 ProcessRepeats=3 UDAObjects=2.048k ValuesSize=256
V8PerfFixture/BM_V8_Recompile_Mutable/8/1024/process_time/real_time                   4678 us         8437 us          156 ProcessRepeats=3 UDAObjects=8 ValuesSize=1.024k
V8PerfFixture/BM_V8_Recompile_Mutable/16/1024/process_time/real_time                  7976 us        14455 us           95 ProcessRepeats=3 UDAObjects=16 ValuesSize=1.024k
V8PerfFixture/BM_V8_Recompile_Mutable/64/1024/process_time/real_time                 32156 us        65469 us           23 ProcessRepeats=3 UDAObjects=64 ValuesSize=1.024k
V8PerfFixture/BM_V8_Recompile_Mutable/256/1024/process_time/real_time               129699 us       209923 us            6 ProcessRepeats=3 UDAObjects=256 ValuesSize=1.024k
V8PerfFixture/BM_V8_Recompile_Mutable/1024/1024/process_time/real_time              467229 us       825653 us            2 ProcessRepeats=3 UDAObjects=1.024k ValuesSize=1.024k
V8PerfFixture/BM_V8_Recompile_Mutable/2048/1024/process_time/real_time             1047781 us      1911897 us            1 ProcessRepeats=3 UDAObjects=2.048k ValuesSize=1.024k
V8PerfFixture/BM_V8_Recompile_Mutable/8/2048/process_time/real_time                   5938 us        12995 us          113 ProcessRepeats=3 UDAObjects=8 ValuesSize=2.048k
V8PerfFixture/BM_V8_Recompile_Mutable/16/2048/process_time/real_time                 11030 us        24148 us           69 ProcessRepeats=3 UDAObjects=16 ValuesSize=2.048k
V8PerfFixture/BM_V8_Recompile_Mutable/64/2048/process_time/real_time                 43131 us       102871 us           16 ProcessRepeats=3 UDAObjects=64 ValuesSize=2.048k
V8PerfFixture/BM_V8_Recompile_Mutable/256/2048/process_time/real_time               170311 us       361997 us            5 ProcessRepeats=3 UDAObjects=256 ValuesSize=2.048k
V8PerfFixture/BM_V8_Recompile_Mutable/1024/2048/process_time/real_time              663945 us      1631863 us            1 ProcessRepeats=3 UDAObjects=1.024k ValuesSize=2.048k
V8PerfFixture/BM_V8_Recompile_Mutable/2048/2048/process_time/real_time             1317127 us      2949035 us            1 ProcessRepeats=3 UDAObjects=2.048k ValuesSize=2.048k
V8PerfFixture/BM_V8_CompileOnce_Create_Mutable/8/64/process_time/real_time            1003 us         1377 us          668 ProcessRepeats=3 UDAObjects=8 ValuesSize=64
V8PerfFixture/BM_V8_CompileOnce_Create_Mutable/16/64/process_time/real_time            991 us         1349 us          677 ProcessRepeats=3 UDAObjects=16 ValuesSize=64
V8PerfFixture/BM_V8_CompileOnce_Create_Mutable/64/64/process_time/real_time           1513 us         1904 us          448 ProcessRepeats=3 UDAObjects=64 ValuesSize=64
V8PerfFixture/BM_V8_CompileOnce_Create_Mutable/256/64/process_time/real_time          3790 us         4776 us          179 ProcessRepeats=3 UDAObjects=256 ValuesSize=64
V8PerfFixture/BM_V8_CompileOnce_Create_Mutable/1024/64/process_time/real_time        12580 us        13977 us           58 ProcessRepeats=3 UDAObjects=1.024k ValuesSize=64
V8PerfFixture/BM_V8_CompileOnce_Create_Mutable/2048/64/process_time/real_time        23143 us        24958 us           30 ProcessRepeats=3 UDAObjects=2.048k ValuesSize=64
V8PerfFixture/BM_V8_CompileOnce_Create_Mutable/8/256/process_time/real_time           1107 us         1545 us          596 ProcessRepeats=3 UDAObjects=8 ValuesSize=256
V8PerfFixture/BM_V8_CompileOnce_Create_Mutable/16/256/process_time/real_time          1382 us         1833 us          500 ProcessRepeats=3 UDAObjects=16 ValuesSize=256
V8PerfFixture/BM_V8_CompileOnce_Create_Mutable/64/256/process_time/real_time          3096 us         4188 us          213 ProcessRepeats=3 UDAObjects=64 ValuesSize=256
V8PerfFixture/BM_V8_CompileOnce_Create_Mutable/256/256/process_time/real_time        10464 us        11875 us           71 ProcessRepeats=3 UDAObjects=256 ValuesSize=256
V8PerfFixture/BM_V8_CompileOnce_Create_Mutable/1024/256/process_time/real_time       37060 us        38977 us           19 ProcessRepeats=3 UDAObjects=1.024k ValuesSize=256
V8PerfFixture/BM_V8_CompileOnce_Create_Mutable/2048/256/process_time/real_time       68473 us        70681 us           10 ProcessRepeats=3 UDAObjects=2.048k ValuesSize=256
V8PerfFixture/BM_V8_CompileOnce_Create_Mutable/8/1024/process_time/real_time          2048 us         3314 us          330 ProcessRepeats=3 UDAObjects=8 ValuesSize=1.024k
V8PerfFixture/BM_V8_CompileOnce_Create_Mutable/16/1024/process_time/real_time         3184 us         4725 us          213 ProcessRepeats=3 UDAObjects=16 ValuesSize=1.024k
V8PerfFixture/BM_V8_CompileOnce_Create_Mutable/64/1024/process_time/real_time         9844 us        11658 us           66 ProcessRepeats=3 UDAObjects=64 ValuesSize=1.024k
V8PerfFixture/BM_V8_CompileOnce_Create_Mutable/256/1024/process_time/real_time       35332 us        37595 us           20 ProcessRepeats=3 UDAObjects=256 ValuesSize=1.024k
V8PerfFixture/BM_V8_CompileOnce_Create_Mutable/1024/1024/process_time/real_time     147896 us       150807 us            4 ProcessRepeats=3 UDAObjects=1.024k ValuesSize=1.024k
V8PerfFixture/BM_V8_CompileOnce_Create_Mutable/2048/1024/process_time/real_time     272246 us       275973 us            3 ProcessRepeats=3 UDAObjects=2.048k ValuesSize=1.024k
V8PerfFixture/BM_V8_CompileOnce_Create_Mutable/8/2048/process_time/real_time          3249 us         5176 us          212 ProcessRepeats=3 UDAObjects=8 ValuesSize=2.048k
V8PerfFixture/BM_V8_CompileOnce_Create_Mutable/16/2048/process_time/real_time         5446 us         7404 us          128 ProcessRepeats=3 UDAObjects=16 ValuesSize=2.048k
V8PerfFixture/BM_V8_CompileOnce_Create_Mutable/64/2048/process_time/real_time        18546 us        20761 us           40 ProcessRepeats=3 UDAObjects=64 ValuesSize=2.048k
V8PerfFixture/BM_V8_CompileOnce_Create_Mutable/256/2048/process_time/real_time       68779 us        71943 us           10 ProcessRepeats=3 UDAObjects=256 ValuesSize=2.048k
V8PerfFixture/BM_V8_CompileOnce_Create_Mutable/1024/2048/process_time/real_time     267064 us       271478 us            3 ProcessRepeats=3 UDAObjects=1.024k ValuesSize=2.048k
V8PerfFixture/BM_V8_CompileOnce_Create_Mutable/2048/2048/process_time/real_time     519743 us       523829 us            1 ProcessRepeats=3 UDAObjects=2.048k ValuesSize=2.048k
V8PerfFixture/BM_V8_CompileOnce_Clone_Mutable/8/64/process_time/real_time              932 us         1298 us          714 ProcessRepeats=3 UDAObjects=8 ValuesSize=64
V8PerfFixture/BM_V8_CompileOnce_Clone_Mutable/16/64/process_time/real_time             950 us         1302 us          855 ProcessRepeats=3 UDAObjects=16 ValuesSize=64
V8PerfFixture/BM_V8_CompileOnce_Clone_Mutable/64/64/process_time/real_time            1458 us         1812 us          448 ProcessRepeats=3 UDAObjects=64 ValuesSize=64
V8PerfFixture/BM_V8_CompileOnce_Clone_Mutable/256/64/process_time/real_time           3744 us         4721 us          187 ProcessRepeats=3 UDAObjects=256 ValuesSize=64
V8PerfFixture/BM_V8_CompileOnce_Clone_Mutable/1024/64/process_time/real_time         12591 us        14016 us           63 ProcessRepeats=3 UDAObjects=1.024k ValuesSize=64
V8PerfFixture/BM_V8_CompileOnce_Clone_Mutable/2048/64/process_time/real_time         25212 us        26775 us           29 ProcessRepeats=3 UDAObjects=2.048k ValuesSize=64
V8PerfFixture/BM_V8_CompileOnce_Clone_Mutable/8/256/process_time/real_time            1023 us         1417 us          679 ProcessRepeats=3 UDAObjects=8 ValuesSize=256
V8PerfFixture/BM_V8_CompileOnce_Clone_Mutable/16/256/process_time/real_time           1267 us         1598 us          550 ProcessRepeats=3 UDAObjects=16 ValuesSize=256
V8PerfFixture/BM_V8_CompileOnce_Clone_Mutable/64/256/process_time/real_time           3096 us         4142 us          226 ProcessRepeats=3 UDAObjects=64 ValuesSize=256
V8PerfFixture/BM_V8_CompileOnce_Clone_Mutable/256/256/process_time/real_time         10084 us        11283 us           67 ProcessRepeats=3 UDAObjects=256 ValuesSize=256
V8PerfFixture/BM_V8_CompileOnce_Clone_Mutable/1024/256/process_time/real_time        37284 us        39278 us           19 ProcessRepeats=3 UDAObjects=1.024k ValuesSize=256
V8PerfFixture/BM_V8_CompileOnce_Clone_Mutable/2048/256/process_time/real_time        71374 us        73536 us           10 ProcessRepeats=3 UDAObjects=2.048k ValuesSize=256
V8PerfFixture/BM_V8_CompileOnce_Clone_Mutable/8/1024/process_time/real_time           1987 us         3264 us          365 ProcessRepeats=3 UDAObjects=8 ValuesSize=1.024k
V8PerfFixture/BM_V8_CompileOnce_Clone_Mutable/16/1024/process_time/real_time          3114 us         4531 us          225 ProcessRepeats=3 UDAObjects=16 ValuesSize=1.024k
V8PerfFixture/BM_V8_CompileOnce_Clone_Mutable/64/1024/process_time/real_time         10016 us        11808 us           71 ProcessRepeats=3 UDAObjects=64 ValuesSize=1.024k
V8PerfFixture/BM_V8_CompileOnce_Clone_Mutable/256/1024/process_time/real_time        34271 us        36640 us           19 ProcessRepeats=3 UDAObjects=256 ValuesSize=1.024k
V8PerfFixture/BM_V8_CompileOnce_Clone_Mutable/1024/1024/process_time/real_time      133731 us       137006 us            5 ProcessRepeats=3 UDAObjects=1.024k ValuesSize=1.024k
V8PerfFixture/BM_V8_CompileOnce_Clone_Mutable/2048/1024/process_time/real_time      269085 us       272834 us            3 ProcessRepeats=3 UDAObjects=2.048k ValuesSize=1.024k
V8PerfFixture/BM_V8_CompileOnce_Clone_Mutable/8/2048/process_time/real_time           3215 us         5124 us          218 ProcessRepeats=3 UDAObjects=8 ValuesSize=2.048k
V8PerfFixture/BM_V8_CompileOnce_Clone_Mutable/16/2048/process_time/real_time          5512 us         7507 us          119 ProcessRepeats=3 UDAObjects=16 ValuesSize=2.048k
V8PerfFixture/BM_V8_CompileOnce_Clone_Mutable/64/2048/process_time/real_time         18042 us        20042 us           38 ProcessRepeats=3 UDAObjects=64 ValuesSize=2.048k
V8PerfFixture/BM_V8_CompileOnce_Clone_Mutable/256/2048/process_time/real_time        66911 us        69702 us           11 ProcessRepeats=3 UDAObjects=256 ValuesSize=2.048k
V8PerfFixture/BM_V8_CompileOnce_Clone_Mutable/1024/2048/process_time/real_time      268278 us       272442 us            3 ProcessRepeats=3 UDAObjects=1.024k ValuesSize=2.048k
V8PerfFixture/BM_V8_CompileOnce_Clone_Mutable/2048/2048/process_time/real_time      523604 us       527289 us            1 ProcessRepeats=3 UDAObjects=2.048k ValuesSize=2.048k
(base) ➜  build_relwith_deb git:(feat/issue-7950-bump-up-v8) ✗ 

```


## old

```
build_relwithdeb git:(develop) ✗ ./src/V8/benchmarks/benchmark_v8_perf 
2025-11-04T06:28:40-08:00
Running ./src/V8/benchmarks/benchmark_v8_perf
Run on (20 X 4900 MHz CPU s)
CPU Caches:
  L1 Data 48 KiB (x10)
  L1 Instruction 32 KiB (x10)
  L2 Unified 1280 KiB (x10)
  L3 Unified 24576 KiB (x1)
Load Average: 2.20, 2.02, 1.94
***WARNING*** CPU scaling is enabled, the benchmark real time measurements may be noisy and will incur extra overhead.
------------------------------------------------------------------------------------------------------------------------------------------
Benchmark                                                                                Time             CPU   Iterations UserCounters...
------------------------------------------------------------------------------------------------------------------------------------------
V8PerfFixture/BM_V8_Recompile/8/64/process_time/real_time                             3909 us         4799 us          188 ProcessRepeats=3 UDAObjects=8 ValuesSize=64
V8PerfFixture/BM_V8_Recompile/16/64/process_time/real_time                            6440 us         7599 us          105 ProcessRepeats=3 UDAObjects=16 ValuesSize=64
V8PerfFixture/BM_V8_Recompile/64/64/process_time/real_time                           24554 us        38411 us           29 ProcessRepeats=3 UDAObjects=64 ValuesSize=64
V8PerfFixture/BM_V8_Recompile/256/64/process_time/real_time                         102107 us       151472 us            7 ProcessRepeats=3 UDAObjects=256 ValuesSize=64
V8PerfFixture/BM_V8_Recompile/1024/64/process_time/real_time                        370141 us       444308 us            2 ProcessRepeats=3 UDAObjects=1.024k ValuesSize=64
V8PerfFixture/BM_V8_Recompile/2048/64/process_time/real_time                        762511 us       931389 us            1 ProcessRepeats=3 UDAObjects=2.048k ValuesSize=64
V8PerfFixture/BM_V8_Recompile/8/256/process_time/real_time                            3968 us         4686 us          182 ProcessRepeats=3 UDAObjects=8 ValuesSize=256
V8PerfFixture/BM_V8_Recompile/16/256/process_time/real_time                           7333 us         8328 us           96 ProcessRepeats=3 UDAObjects=16 ValuesSize=256
V8PerfFixture/BM_V8_Recompile/64/256/process_time/real_time                          29322 us        42255 us           26 ProcessRepeats=3 UDAObjects=64 ValuesSize=256
V8PerfFixture/BM_V8_Recompile/256/256/process_time/real_time                        109748 us       148765 us            7 ProcessRepeats=3 UDAObjects=256 ValuesSize=256
V8PerfFixture/BM_V8_Recompile/1024/256/process_time/real_time                       381066 us       447856 us            2 ProcessRepeats=3 UDAObjects=1.024k ValuesSize=256
V8PerfFixture/BM_V8_Recompile/2048/256/process_time/real_time                       861037 us      1045855 us            1 ProcessRepeats=3 UDAObjects=2.048k ValuesSize=256
V8PerfFixture/BM_V8_Recompile/8/1024/process_time/real_time                           5390 us         5974 us          130 ProcessRepeats=3 UDAObjects=8 ValuesSize=1.024k
V8PerfFixture/BM_V8_Recompile/16/1024/process_time/real_time                          9674 us        10526 us           74 ProcessRepeats=3 UDAObjects=16 ValuesSize=1.024k
V8PerfFixture/BM_V8_Recompile/64/1024/process_time/real_time                         35003 us        46040 us           19 ProcessRepeats=3 UDAObjects=64 ValuesSize=1.024k
V8PerfFixture/BM_V8_Recompile/256/1024/process_time/real_time                       141109 us       178355 us            6 ProcessRepeats=3 UDAObjects=256 ValuesSize=1.024k
V8PerfFixture/BM_V8_Recompile/1024/1024/process_time/real_time                      529011 us       596090 us            1 ProcessRepeats=3 UDAObjects=1.024k ValuesSize=1.024k
V8PerfFixture/BM_V8_Recompile/2048/1024/process_time/real_time                     1270870 us      1474307 us            1 ProcessRepeats=3 UDAObjects=2.048k ValuesSize=1.024k
V8PerfFixture/BM_V8_Recompile/8/2048/process_time/real_time                           7558 us         8387 us           87 ProcessRepeats=3 UDAObjects=8 ValuesSize=2.048k
V8PerfFixture/BM_V8_Recompile/16/2048/process_time/real_time                         14594 us        15795 us           52 ProcessRepeats=3 UDAObjects=16 ValuesSize=2.048k
V8PerfFixture/BM_V8_Recompile/64/2048/process_time/real_time                         60602 us        73926 us           13 ProcessRepeats=3 UDAObjects=64 ValuesSize=2.048k
V8PerfFixture/BM_V8_Recompile/256/2048/process_time/real_time                       227813 us       257644 us            3 ProcessRepeats=3 UDAObjects=256 ValuesSize=2.048k
V8PerfFixture/BM_V8_Recompile/1024/2048/process_time/real_time                      801525 us       877185 us            1 ProcessRepeats=3 UDAObjects=1.024k ValuesSize=2.048k
V8PerfFixture/BM_V8_Recompile/2048/2048/process_time/real_time                     1650017 us      1862153 us            1 ProcessRepeats=3 UDAObjects=2.048k ValuesSize=2.048k
V8PerfFixture/BM_V8_CompileOnce_Create/8/64/process_time/real_time                    1568 us         2668 us          467 ProcessRepeats=3 UDAObjects=8 ValuesSize=64
V8PerfFixture/BM_V8_CompileOnce_Create/16/64/process_time/real_time                   1761 us         2852 us          408 ProcessRepeats=3 UDAObjects=16 ValuesSize=64
V8PerfFixture/BM_V8_CompileOnce_Create/64/64/process_time/real_time                   2777 us         3881 us          274 ProcessRepeats=3 UDAObjects=64 ValuesSize=64
V8PerfFixture/BM_V8_CompileOnce_Create/256/64/process_time/real_time                  6994 us         8189 us           94 ProcessRepeats=3 UDAObjects=256 ValuesSize=64
V8PerfFixture/BM_V8_CompileOnce_Create/1024/64/process_time/real_time                20807 us        22251 us           34 ProcessRepeats=3 UDAObjects=1.024k ValuesSize=64
V8PerfFixture/BM_V8_CompileOnce_Create/2048/64/process_time/real_time                36270 us        37806 us           18 ProcessRepeats=3 UDAObjects=2.048k ValuesSize=64
V8PerfFixture/BM_V8_CompileOnce_Create/8/256/process_time/real_time                   1969 us         3061 us          347 ProcessRepeats=3 UDAObjects=8 ValuesSize=256
V8PerfFixture/BM_V8_CompileOnce_Create/16/256/process_time/real_time                  2778 us         3924 us          288 ProcessRepeats=3 UDAObjects=16 ValuesSize=256
V8PerfFixture/BM_V8_CompileOnce_Create/64/256/process_time/real_time                  6586 us         7848 us          110 ProcessRepeats=3 UDAObjects=64 ValuesSize=256
V8PerfFixture/BM_V8_CompileOnce_Create/256/256/process_time/real_time                18575 us        20139 us           39 ProcessRepeats=3 UDAObjects=256 ValuesSize=256
V8PerfFixture/BM_V8_CompileOnce_Create/1024/256/process_time/real_time               58042 us        59559 us           11 ProcessRepeats=3 UDAObjects=1.024k ValuesSize=256
V8PerfFixture/BM_V8_CompileOnce_Create/2048/256/process_time/real_time              117548 us       118924 us            6 ProcessRepeats=3 UDAObjects=2.048k ValuesSize=256
V8PerfFixture/BM_V8_CompileOnce_Create/8/1024/process_time/real_time                  3972 us         5184 us          186 ProcessRepeats=3 UDAObjects=8 ValuesSize=1.024k
V8PerfFixture/BM_V8_CompileOnce_Create/16/1024/process_time/real_time                 6201 us         7380 us          114 ProcessRepeats=3 UDAObjects=16 ValuesSize=1.024k
V8PerfFixture/BM_V8_CompileOnce_Create/64/1024/process_time/real_time                18245 us        19668 us           39 ProcessRepeats=3 UDAObjects=64 ValuesSize=1.024k
V8PerfFixture/BM_V8_CompileOnce_Create/256/1024/process_time/real_time               56839 us        58372 us           11 ProcessRepeats=3 UDAObjects=256 ValuesSize=1.024k
V8PerfFixture/BM_V8_CompileOnce_Create/1024/1024/process_time/real_time             219700 us       221193 us            3 ProcessRepeats=3 UDAObjects=1.024k ValuesSize=1.024k
V8PerfFixture/BM_V8_CompileOnce_Create/2048/1024/process_time/real_time             405339 us       406960 us            2 ProcessRepeats=3 UDAObjects=2.048k ValuesSize=1.024k
V8PerfFixture/BM_V8_CompileOnce_Create/8/2048/process_time/real_time                  6296 us         7325 us          107 ProcessRepeats=3 UDAObjects=8 ValuesSize=2.048k
V8PerfFixture/BM_V8_CompileOnce_Create/16/2048/process_time/real_time                10815 us        12205 us           65 ProcessRepeats=3 UDAObjects=16 ValuesSize=2.048k
V8PerfFixture/BM_V8_CompileOnce_Create/64/2048/process_time/real_time                30009 us        31566 us           20 ProcessRepeats=3 UDAObjects=64 ValuesSize=2.048k
V8PerfFixture/BM_V8_CompileOnce_Create/256/2048/process_time/real_time              105889 us       107528 us            7 ProcessRepeats=3 UDAObjects=256 ValuesSize=2.048k
V8PerfFixture/BM_V8_CompileOnce_Create/1024/2048/process_time/real_time             409304 us       410725 us            2 ProcessRepeats=3 UDAObjects=1.024k ValuesSize=2.048k
V8PerfFixture/BM_V8_CompileOnce_Create/2048/2048/process_time/real_time             770031 us       772257 us            1 ProcessRepeats=3 UDAObjects=2.048k ValuesSize=2.048k
V8PerfFixture/BM_V8_CompileOnce_Clone/8/64/process_time/real_time                     1543 us         2642 us          496 ProcessRepeats=3 UDAObjects=8 ValuesSize=64
V8PerfFixture/BM_V8_CompileOnce_Clone/16/64/process_time/real_time                    1564 us         2613 us          454 ProcessRepeats=3 UDAObjects=16 ValuesSize=64
V8PerfFixture/BM_V8_CompileOnce_Clone/64/64/process_time/real_time                    2829 us         3953 us          272 ProcessRepeats=3 UDAObjects=64 ValuesSize=64
V8PerfFixture/BM_V8_CompileOnce_Clone/256/64/process_time/real_time                   7772 us         9070 us           94 ProcessRepeats=3 UDAObjects=256 ValuesSize=64
V8PerfFixture/BM_V8_CompileOnce_Clone/1024/64/process_time/real_time                 22154 us        23906 us           31 ProcessRepeats=3 UDAObjects=1.024k ValuesSize=64
V8PerfFixture/BM_V8_CompileOnce_Clone/2048/64/process_time/real_time                 34729 us        36418 us           18 ProcessRepeats=3 UDAObjects=2.048k ValuesSize=64
V8PerfFixture/BM_V8_CompileOnce_Clone/8/256/process_time/real_time                    2010 us         3117 us          376 ProcessRepeats=3 UDAObjects=8 ValuesSize=256
V8PerfFixture/BM_V8_CompileOnce_Clone/16/256/process_time/real_time                   2391 us         3455 us          289 ProcessRepeats=3 UDAObjects=16 ValuesSize=256
V8PerfFixture/BM_V8_CompileOnce_Clone/64/256/process_time/real_time                   6439 us         7534 us          103 ProcessRepeats=3 UDAObjects=64 ValuesSize=256
V8PerfFixture/BM_V8_CompileOnce_Clone/256/256/process_time/real_time                 16573 us        17828 us           40 ProcessRepeats=3 UDAObjects=256 ValuesSize=256
V8PerfFixture/BM_V8_CompileOnce_Clone/1024/256/process_time/real_time                57505 us        59177 us           11 ProcessRepeats=3 UDAObjects=1.024k ValuesSize=256
V8PerfFixture/BM_V8_CompileOnce_Clone/2048/256/process_time/real_time               110977 us       112666 us            6 ProcessRepeats=3 UDAObjects=2.048k ValuesSize=256
V8PerfFixture/BM_V8_CompileOnce_Clone/8/1024/process_time/real_time                   3667 us         4840 us          186 ProcessRepeats=3 UDAObjects=8 ValuesSize=1.024k
V8PerfFixture/BM_V8_CompileOnce_Clone/16/1024/process_time/real_time                  6363 us         7533 us           90 ProcessRepeats=3 UDAObjects=16 ValuesSize=1.024k
V8PerfFixture/BM_V8_CompileOnce_Clone/64/1024/process_time/real_time                 17476 us        18799 us           42 ProcessRepeats=3 UDAObjects=64 ValuesSize=1.024k
V8PerfFixture/BM_V8_CompileOnce_Clone/256/1024/process_time/real_time                55242 us        56708 us           11 ProcessRepeats=3 UDAObjects=256 ValuesSize=1.024k
V8PerfFixture/BM_V8_CompileOnce_Clone/1024/1024/process_time/real_time              215248 us       216449 us            3 ProcessRepeats=3 UDAObjects=1.024k ValuesSize=1.024k
V8PerfFixture/BM_V8_CompileOnce_Clone/2048/1024/process_time/real_time              409277 us       410833 us            2 ProcessRepeats=3 UDAObjects=2.048k ValuesSize=1.024k
V8PerfFixture/BM_V8_CompileOnce_Clone/8/2048/process_time/real_time                   6327 us         7309 us          109 ProcessRepeats=3 UDAObjects=8 ValuesSize=2.048k
V8PerfFixture/BM_V8_CompileOnce_Clone/16/2048/process_time/real_time                 10058 us        11332 us           67 ProcessRepeats=3 UDAObjects=16 ValuesSize=2.048k
V8PerfFixture/BM_V8_CompileOnce_Clone/64/2048/process_time/real_time                 29767 us        31250 us           24 ProcessRepeats=3 UDAObjects=64 ValuesSize=2.048k
V8PerfFixture/BM_V8_CompileOnce_Clone/256/2048/process_time/real_time               104870 us       106316 us            7 ProcessRepeats=3 UDAObjects=256 ValuesSize=2.048k
V8PerfFixture/BM_V8_CompileOnce_Clone/1024/2048/process_time/real_time              404231 us       405692 us            2 ProcessRepeats=3 UDAObjects=1.024k ValuesSize=2.048k
V8PerfFixture/BM_V8_CompileOnce_Clone/2048/2048/process_time/real_time              767119 us       768120 us            1 ProcessRepeats=3 UDAObjects=2.048k ValuesSize=2.048k
V8PerfFixture/BM_V8_Recompile_Mutable/8/64/process_time/real_time                     3331 us         4069 us          205 ProcessRepeats=3 UDAObjects=8 ValuesSize=64
V8PerfFixture/BM_V8_Recompile_Mutable/16/64/process_time/real_time                    5778 us         6808 us          122 ProcessRepeats=3 UDAObjects=16 ValuesSize=64
V8PerfFixture/BM_V8_Recompile_Mutable/64/64/process_time/real_time                   21286 us        33710 us           34 ProcessRepeats=3 UDAObjects=64 ValuesSize=64
V8PerfFixture/BM_V8_Recompile_Mutable/256/64/process_time/real_time                  89431 us       132560 us           10 ProcessRepeats=3 UDAObjects=256 ValuesSize=64
V8PerfFixture/BM_V8_Recompile_Mutable/1024/64/process_time/real_time                346897 us       421054 us            2 ProcessRepeats=3 UDAObjects=1.024k ValuesSize=64
V8PerfFixture/BM_V8_Recompile_Mutable/2048/64/process_time/real_time                768743 us       968515 us            1 ProcessRepeats=3 UDAObjects=2.048k ValuesSize=64
V8PerfFixture/BM_V8_Recompile_Mutable/8/256/process_time/real_time                    4232 us         4954 us          187 ProcessRepeats=3 UDAObjects=8 ValuesSize=256
V8PerfFixture/BM_V8_Recompile_Mutable/16/256/process_time/real_time                   6929 us         7860 us           89 ProcessRepeats=3 UDAObjects=16 ValuesSize=256
V8PerfFixture/BM_V8_Recompile_Mutable/64/256/process_time/real_time                  24212 us        36333 us           27 ProcessRepeats=3 UDAObjects=64 ValuesSize=256
V8PerfFixture/BM_V8_Recompile_Mutable/256/256/process_time/real_time                103593 us       146206 us            8 ProcessRepeats=3 UDAObjects=256 ValuesSize=256
V8PerfFixture/BM_V8_Recompile_Mutable/1024/256/process_time/real_time               382132 us       454693 us            2 ProcessRepeats=3 UDAObjects=1.024k ValuesSize=256
V8PerfFixture/BM_V8_Recompile_Mutable/2048/256/process_time/real_time               847716 us      1051714 us            1 ProcessRepeats=3 UDAObjects=2.048k ValuesSize=256
V8PerfFixture/BM_V8_Recompile_Mutable/8/1024/process_time/real_time                   5541 us         6169 us          124 ProcessRepeats=3 UDAObjects=8 ValuesSize=1.024k
V8PerfFixture/BM_V8_Recompile_Mutable/16/1024/process_time/real_time                 10012 us        10976 us           70 ProcessRepeats=3 UDAObjects=16 ValuesSize=1.024k
V8PerfFixture/BM_V8_Recompile_Mutable/64/1024/process_time/real_time                 38510 us        51608 us           19 ProcessRepeats=3 UDAObjects=64 ValuesSize=1.024k
V8PerfFixture/BM_V8_Recompile_Mutable/256/1024/process_time/real_time               170846 us       216625 us            5 ProcessRepeats=3 UDAObjects=256 ValuesSize=1.024k
V8PerfFixture/BM_V8_Recompile_Mutable/1024/1024/process_time/real_time              570799 us       648546 us            1 ProcessRepeats=3 UDAObjects=1.024k ValuesSize=1.024k
V8PerfFixture/BM_V8_Recompile_Mutable/2048/1024/process_time/real_time             1234735 us      1435664 us            1 ProcessRepeats=3 UDAObjects=2.048k ValuesSize=1.024k
V8PerfFixture/BM_V8_Recompile_Mutable/8/2048/process_time/real_time                   7629 us         8514 us           84 ProcessRepeats=3 UDAObjects=8 ValuesSize=2.048k
V8PerfFixture/BM_V8_Recompile_Mutable/16/2048/process_time/real_time                 14063 us        15615 us           49 ProcessRepeats=3 UDAObjects=16 ValuesSize=2.048k
V8PerfFixture/BM_V8_Recompile_Mutable/64/2048/process_time/real_time                 53008 us        65203 us           10 ProcessRepeats=3 UDAObjects=64 ValuesSize=2.048k
V8PerfFixture/BM_V8_Recompile_Mutable/256/2048/process_time/real_time               232877 us       263220 us            4 ProcessRepeats=3 UDAObjects=256 ValuesSize=2.048k
V8PerfFixture/BM_V8_Recompile_Mutable/1024/2048/process_time/real_time              766514 us       841428 us            1 ProcessRepeats=3 UDAObjects=1.024k ValuesSize=2.048k
V8PerfFixture/BM_V8_Recompile_Mutable/2048/2048/process_time/real_time             1811709 us      2040424 us            1 ProcessRepeats=3 UDAObjects=2.048k ValuesSize=2.048k
V8PerfFixture/BM_V8_CompileOnce_Create_Mutable/8/64/process_time/real_time            1524 us         2624 us          443 ProcessRepeats=3 UDAObjects=8 ValuesSize=64
V8PerfFixture/BM_V8_CompileOnce_Create_Mutable/16/64/process_time/real_time           2021 us         3603 us          355 ProcessRepeats=3 UDAObjects=16 ValuesSize=64
V8PerfFixture/BM_V8_CompileOnce_Create_Mutable/64/64/process_time/real_time           2699 us         3794 us          258 ProcessRepeats=3 UDAObjects=64 ValuesSize=64
V8PerfFixture/BM_V8_CompileOnce_Create_Mutable/256/64/process_time/real_time          7412 us         8605 us           91 ProcessRepeats=3 UDAObjects=256 ValuesSize=64
V8PerfFixture/BM_V8_CompileOnce_Create_Mutable/1024/64/process_time/real_time        20009 us        21532 us           36 ProcessRepeats=3 UDAObjects=1.024k ValuesSize=64
V8PerfFixture/BM_V8_CompileOnce_Create_Mutable/2048/64/process_time/real_time        36841 us        38728 us           19 ProcessRepeats=3 UDAObjects=2.048k ValuesSize=64
V8PerfFixture/BM_V8_CompileOnce_Create_Mutable/8/256/process_time/real_time           1915 us         2968 us          368 ProcessRepeats=3 UDAObjects=8 ValuesSize=256
V8PerfFixture/BM_V8_CompileOnce_Create_Mutable/16/256/process_time/real_time          2581 us         3666 us          275 ProcessRepeats=3 UDAObjects=16 ValuesSize=256
V8PerfFixture/BM_V8_CompileOnce_Create_Mutable/64/256/process_time/real_time          6811 us         7981 us           96 ProcessRepeats=3 UDAObjects=64 ValuesSize=256
V8PerfFixture/BM_V8_CompileOnce_Create_Mutable/256/256/process_time/real_time        17961 us        19295 us           38 ProcessRepeats=3 UDAObjects=256 ValuesSize=256
V8PerfFixture/BM_V8_CompileOnce_Create_Mutable/1024/256/process_time/real_time       61077 us        62834 us           11 ProcessRepeats=3 UDAObjects=1.024k ValuesSize=256
V8PerfFixture/BM_V8_CompileOnce_Create_Mutable/2048/256/process_time/real_time      118044 us       120299 us            6 ProcessRepeats=3 UDAObjects=2.048k ValuesSize=256
V8PerfFixture/BM_V8_CompileOnce_Create_Mutable/8/1024/process_time/real_time          3988 us         5015 us          174 ProcessRepeats=3 UDAObjects=8 ValuesSize=1.024k
V8PerfFixture/BM_V8_CompileOnce_Create_Mutable/16/1024/process_time/real_time         6854 us         7976 us           99 ProcessRepeats=3 UDAObjects=16 ValuesSize=1.024k
V8PerfFixture/BM_V8_CompileOnce_Create_Mutable/64/1024/process_time/real_time        17618 us        18962 us           38 ProcessRepeats=3 UDAObjects=64 ValuesSize=1.024k
V8PerfFixture/BM_V8_CompileOnce_Create_Mutable/256/1024/process_time/real_time       65573 us        67220 us           11 ProcessRepeats=3 UDAObjects=256 ValuesSize=1.024k
V8PerfFixture/BM_V8_CompileOnce_Create_Mutable/1024/1024/process_time/real_time     224578 us       226278 us            3 ProcessRepeats=3 UDAObjects=1.024k ValuesSize=1.024k
V8PerfFixture/BM_V8_CompileOnce_Create_Mutable/2048/1024/process_time/real_time     463245 us       465156 us            2 ProcessRepeats=3 UDAObjects=2.048k ValuesSize=1.024k
V8PerfFixture/BM_V8_CompileOnce_Create_Mutable/8/2048/process_time/real_time          6924 us         8014 us           98 ProcessRepeats=3 UDAObjects=8 ValuesSize=2.048k
V8PerfFixture/BM_V8_CompileOnce_Create_Mutable/16/2048/process_time/real_time        11023 us        12337 us           62 ProcessRepeats=3 UDAObjects=16 ValuesSize=2.048k
V8PerfFixture/BM_V8_CompileOnce_Create_Mutable/64/2048/process_time/real_time        31928 us        33323 us           22 ProcessRepeats=3 UDAObjects=64 ValuesSize=2.048k
V8PerfFixture/BM_V8_CompileOnce_Create_Mutable/256/2048/process_time/real_time      111408 us       113005 us            6 ProcessRepeats=3 UDAObjects=256 ValuesSize=2.048k
V8PerfFixture/BM_V8_CompileOnce_Create_Mutable/1024/2048/process_time/real_time     429443 us       431407 us            2 ProcessRepeats=3 UDAObjects=1.024k ValuesSize=2.048k
V8PerfFixture/BM_V8_CompileOnce_Create_Mutable/2048/2048/process_time/real_time     854276 us       857115 us            1 ProcessRepeats=3 UDAObjects=2.048k ValuesSize=2.048k
V8PerfFixture/BM_V8_CompileOnce_Clone_Mutable/8/64/process_time/real_time             1431 us         2472 us          487 ProcessRepeats=3 UDAObjects=8 ValuesSize=64
V8PerfFixture/BM_V8_CompileOnce_Clone_Mutable/16/64/process_time/real_time            1641 us         2777 us          416 ProcessRepeats=3 UDAObjects=16 ValuesSize=64
V8PerfFixture/BM_V8_CompileOnce_Clone_Mutable/64/64/process_time/real_time            2727 us         3823 us          253 ProcessRepeats=3 UDAObjects=64 ValuesSize=64
V8PerfFixture/BM_V8_CompileOnce_Clone_Mutable/256/64/process_time/real_time           7467 us         8592 us           88 ProcessRepeats=3 UDAObjects=256 ValuesSize=64
V8PerfFixture/BM_V8_CompileOnce_Clone_Mutable/1024/64/process_time/real_time         20554 us        22288 us           34 ProcessRepeats=3 UDAObjects=1.024k ValuesSize=64
V8PerfFixture/BM_V8_CompileOnce_Clone_Mutable/2048/64/process_time/real_time         36563 us        38449 us           18 ProcessRepeats=3 UDAObjects=2.048k ValuesSize=64
V8PerfFixture/BM_V8_CompileOnce_Clone_Mutable/8/256/process_time/real_time            2114 us         3243 us          353 ProcessRepeats=3 UDAObjects=8 ValuesSize=256
V8PerfFixture/BM_V8_CompileOnce_Clone_Mutable/16/256/process_time/real_time           2607 us         3666 us          270 ProcessRepeats=3 UDAObjects=16 ValuesSize=256
V8PerfFixture/BM_V8_CompileOnce_Clone_Mutable/64/256/process_time/real_time           6925 us         8067 us          101 ProcessRepeats=3 UDAObjects=64 ValuesSize=256
V8PerfFixture/BM_V8_CompileOnce_Clone_Mutable/256/256/process_time/real_time         20292 us        21796 us           31 ProcessRepeats=3 UDAObjects=256 ValuesSize=256
V8PerfFixture/BM_V8_CompileOnce_Clone_Mutable/1024/256/process_time/real_time        71254 us        73094 us           10 ProcessRepeats=3 UDAObjects=1.024k ValuesSize=256
V8PerfFixture/BM_V8_CompileOnce_Clone_Mutable/2048/256/process_time/real_time       132365 us       134831 us            5 ProcessRepeats=3 UDAObjects=2.048k ValuesSize=256
V8PerfFixture/BM_V8_CompileOnce_Clone_Mutable/8/1024/process_time/real_time           4201 us         5325 us          132 ProcessRepeats=3 UDAObjects=8 ValuesSize=1.024k
V8PerfFixture/BM_V8_CompileOnce_Clone_Mutable/16/1024/process_time/real_time          7574 us         8834 us           89 ProcessRepeats=3 UDAObjects=16 ValuesSize=1.024k
V8PerfFixture/BM_V8_CompileOnce_Clone_Mutable/64/1024/process_time/real_time         19308 us        20764 us           38 ProcessRepeats=3 UDAObjects=64 ValuesSize=1.024k
V8PerfFixture/BM_V8_CompileOnce_Clone_Mutable/256/1024/process_time/real_time        64877 us        66397 us           10 ProcessRepeats=3 UDAObjects=256 ValuesSize=1.024k
V8PerfFixture/BM_V8_CompileOnce_Clone_Mutable/1024/1024/process_time/real_time      213814 us       215153 us            3 ProcessRepeats=3 UDAObjects=1.024k ValuesSize=1.024k
V8PerfFixture/BM_V8_CompileOnce_Clone_Mutable/2048/1024/process_time/real_time      438854 us       441765 us            2 ProcessRepeats=3 UDAObjects=2.048k ValuesSize=1.024k
V8PerfFixture/BM_V8_CompileOnce_Clone_Mutable/8/2048/process_time/real_time           6843 us         7938 us           97 ProcessRepeats=3 UDAObjects=8 ValuesSize=2.048k
V8PerfFixture/BM_V8_CompileOnce_Clone_Mutable/16/2048/process_time/real_time         10868 us        12097 us           64 ProcessRepeats=3 UDAObjects=16 ValuesSize=2.048k
V8PerfFixture/BM_V8_CompileOnce_Clone_Mutable/64/2048/process_time/real_time         32164 us        33654 us           22 ProcessRepeats=3 UDAObjects=64 ValuesSize=2.048k
V8PerfFixture/BM_V8_CompileOnce_Clone_Mutable/256/2048/process_time/real_time       112293 us       113937 us            6 ProcessRepeats=3 UDAObjects=256 ValuesSize=2.048k
V8PerfFixture/BM_V8_CompileOnce_Clone_Mutable/1024/2048/process_time/real_time      437300 us       439080 us            2 ProcessRepeats=3 UDAObjects=1.024k ValuesSize=2.048k
V8PerfFixture/BM_V8_CompileOnce_Clone_Mutable/2048/2048/process_time/real_time      863474 us       866644 us            1 ProcessRepeats=3 UDAObjects=2.048k ValuesSize=2.048k
➜  build_relwithdeb git:(develop) ✗ 
```
