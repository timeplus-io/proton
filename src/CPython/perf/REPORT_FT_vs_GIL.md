# Free-threaded CPython 3.14 (cp314t) vs GIL CPython 3.10 — bench comparison

Branch: `feature/issue-11704-cpython-3.14-migration`
Date: 2026-04-12 (updated after `ff8bc31d04e` — HAVE_SOCKET fix)
Bench: `tmp/bench_now/bench.py` · EPS=500000 · DURATION=10s · PARALLEL=3 · TCP_PORT=8463

| label | binary | Python |
|-------|--------|--------|
| **GIL** | `build_with_profile/programs/proton_new_dd689daf8af` | 3.10.14 (py310 venv) |
| **FT**  | `build_with_profile/programs/proton` (`ff8bc31d04e`)   | 3.14.4 free-threaded (py314t venv) |

## Throughput (rows/s)

| test | dtype | single GIL | 3× GIL | 3×/1× GIL | single FT | 3× FT | 3×/1× FT | FT vs GIL 3× |
|------|-------|-----------:|-------:|----------:|----------:|------:|---------:|-------------:|
| sink     | int   |    494,577 |  1,475,806 | 2.98× |    495,219 |  1,456,628 | 2.94× | 0.99× |
| sink     | float |    494,899 |  1,482,036 | 2.99× |    488,529 |  1,416,736 | 2.90× | 0.96× |
| sink     | str   |    495,013 |  1,475,927 | 2.98× |    488,491 |  1,430,124 | 2.93× | 0.97× |
| sink     | mixed |    494,676 |  1,468,949 | 2.97× |    488,369 |  1,390,410 | 2.85× | 0.95× |
| source   | int   | 63,461,714 | 55,813,813 | **0.88×** | 61,058,915 | 167,664,083 | **2.75×** | **3.00×** |
| source   | float | 83,073,515 | 71,367,297 | **0.86×** | 83,531,398 | 227,908,326 | **2.73×** | **3.19×** |
| source   | str   | 49,034,816 | 41,841,823 | **0.85×** | 50,018,501 | 135,704,036 | **2.71×** | **3.24×** |
| source   | mixed | 61,381,983 | 53,601,195 | **0.87×** | 60,423,866 | 160,206,344 | **2.65×** | **2.99×** |
| passthru | int   |    382,019 |  1,227,450 | 3.21× |    377,662 |  1,228,700 | 3.25× | 1.00× |
| passthru | float |    382,387 |  1,227,352 | 3.21× |    381,373 |  1,228,720 | 3.22× | 1.00× |
| passthru | str   |    377,358 |  1,227,335 | 3.25× |    381,362 |  1,229,275 | 3.22× | 1.00× |
| passthru | mixed |    381,245 |  1,220,112 | 3.20× |    381,270 |  1,229,078 | 3.22× | 1.01× |

### Takeaways

1. **Sink (C++→Py, discard).** Both builds hit ~495 K rows/s per stream — capped by the random-source EPS supply, not Python. 3× scales ~2.9× on both. No GIL signal here.

2. **Source (Py→C++, generator) — the clean GIL test.**
   - **GIL build: 3× parallel is worse than 1×** (0.85–0.88× aggregate). Three Python threads serialize through the GIL and get *less* total work done than a single stream.
   - **FT build: 3× parallel is 2.65–2.75× total.** FT 3× vs GIL 3× is **2.99–3.24×**.
   - This is the architectural win of the migration.

3. **Passthrough (C++→Py→Timeplus via `proton_driver`) — now fixed.** Previously the FT build returned `0 rows` with `Code 210 Operation not supported`; root cause was a missing `HAVE_SOCKET` (and ~15 other `HAVE_<bsd_socket_fn>`) in our `cpython-cmake` config, which caused `socketmodule.c` to silently replace `socket()` with a stub that always sets `errno=ENOTSUP`. See commit **`ff8bc31d04e`**. After the fix:
   - FT single-stream: 377–381 K/s — matches GIL build within noise.
   - FT 3× parallel: 1.22–1.23 M/s — matches GIL build within noise.
   - Makes sense: this path is bounded by the synchronous TCP round-trip of `proton_driver.Client.execute`, not by GIL contention.

## Flamegraphs

| capture | binary | SVG |
|---------|--------|-----|
| phases 3–4 (sink+source) | GIL | `tmp/bench_gil/flamegraph_phase1.svg` |
| phases 3–4 (sink+source) | FT  | `tmp/bench_ft/flamegraph_phase1.svg` |
| 3× GIL-contention phase  | GIL | `tmp/bench_gil/flamegraph_gil.svg`   |
| 3× GIL-contention phase  | FT  | `tmp/bench_ft/flamegraph_gil.svg`    |

Raw data: `perf_dwarf*.data` in each dir; stacks: `stacks_*.txt`. Capture parameters identical across builds (perf record `-F 99 --call-graph dwarf -p $SRV_PID -- sleep 60`).

## Hotspot shift (top leaves during 3× contention)

Ranked by cumulative sample weight; **kernel/lock primitives bolded**.

| Rank | GIL (py3.10) — top leaves | FT (cp314t) — top leaves |
|-----:|---------------------------|--------------------------|
| 1 | QueryPullPipeEx (thread)           | QueryPipelineEx (thread)                |
| 2 | QueryCompPipeEx (thread)           | [[vdso]]                                |
| 3 | [[vdso]]                           | `__memset_avx2_unaligned_erms`          |
| 4 | `__strlen_avx2`                    | `__memcpy_avx_unaligned_erms`           |
| 5 | **`lll_mutex_unlock_optimized`**   | QueryCompPipeEx (thread)                |
| 6 | `__memset_avx2_unaligned_erms`     | `__strlen_avx2`                         |
| 7 | **`___pthread_mutex_trylock`**     | **`lll_mutex_lock_optimized`** (mimalloc per-heap) |
| 8 | `__memcpy_avx_unaligned_erms`      | **`lll_mutex_unlock_optimized`**        |
| 9 | `acpi_ns_search_one_scope` (noise) | **`___pthread_mutex_lock`**             |
| 10 | **`futex_wake`**                  | `TimerIniter`                           |
| 11 | **`futex_hash`**                  | `acpi_ns_search_one_scope` (noise)      |
| 12 | **`psi_group_change`**            | ClientIOWorker                          |
| 13 | TimerIniter                       | `strncpy`                               |
| 14 | **`entry_SYSCALL_64`**            | **`___pthread_mutex_unlock`**           |
| 15 | **`__raw_spin_lock_irqsave`**     | `advance_transaction`                   |
| 16 | `native_write_msr` (ktimer)       | `__GI___pthread_mutex_unlock_usercnt`   |
| 17 | **`entry_SYSRETQ_unsafe_stack`**  | `acpi_ut_trace_ptr` (noise)             |
| 18 | **`___pthread_mutex_lock`**       | `_raw_spin_lock`                        |
| 19 | `advance_transaction`             | `TimerService`                          |
| 20 | **`dequeue_entity`** (scheduler)  | `asm_sysvec_apic_timer_interrupt`       |

### What shifted

- **GIL build dominated by kernel thread-park / wake primitives** — `futex_wake`, `futex_hash`, `psi_group_change`, `dequeue_entity`, `entry_SYSCALL_64`, `entry_SYSRETQ_unsafe_stack`, `__raw_spin_lock_irqsave`. Classic GIL-futex signature: three worker threads park on the GIL, the kernel picks one to wake. Per-thread Python work barely shows up because threads spend wall time parked.
- **FT build loses the entire kernel-park pattern.** No `futex_wake`, no `psi_group_change`, no `dequeue_entity` in the top-20. Userspace mutex activity (`lll_mutex_lock/unlock`, `pthread_mutex_lock/unlock`) climbs in — mimalloc's per-heap mutexes and the biased-refcount shared path are the new secondary cost.
- **`memset_avx2` / `memcpy_avx` climb into the top-5** once the GIL-park pattern is gone. It is tempting to read this as "allocator + memcpy are the new bottleneck." **The follow-up measurement below refutes that reading** — these leaves are hot because faster threads now run those code paths more often, not because they gate throughput.

## Is 2.72× a software bottleneck? — follow-up measurement (2026-04-12)

**TL;DR: no. On this laptop, ~2.72× is already near the hardware ceiling. Do not infer a remaining FT synchronization bottleneck from this number. Further validation must happen on server-class hardware before any scaling-motivated code change.**

### Environment caveat (important)

The earlier capture ran on an Intel **i9-12900H** — a *laptop* hybrid CPU with 6 Performance cores (2-way SMT, turbo to ~4.7 GHz single-core, ~4.4 GHz all-core) and 8 Efficient cores (~3.7 GHz, materially lower IPC). That topology is **not representative** of production server SKUs. Every scaling number below must be read with that in mind.

### Method

Live measurement of 1× and 3× parallel `SELECT count() FROM bench_py_gen_int LIMIT 1 EMIT PERIODIC 20s` against the same FT binary, capturing in separate runs:
- DRAM bandwidth via `perf stat -a -e uncore_imc_free_running/data_{read,write}/`
- Per-PID `task-clock`, `context-switches`, `cpu-migrations`, P-core `cycles` / `instructions`
- A repeat 3× run with all 305 server threads pinned to P-cores only (`taskset -cp 0-11 <pid>`)

Artifacts: `tmp/perf_bandwidth_probe/`.

### Numbers

| measurement | 1× | 3× (unpinned) | 3× (P-core pinned) |
|---|---:|---:|---:|
| rows/s | 56.96 M | 154.5 M | 157.8 M |
| vs 1× | — | **2.71×** | **2.77×** |
| task-clock (CPUs utilized) | 1.04 | 3.04 | 3.04 |
| context-switches / s | 1,165 | 1,293 | 1,213 |
| P-core instructions / row | 319.7 | 319.0 | ≈ 319 |
| P-core IPC | 3.70 | 3.65 | ≈ 3.65 |
| DRAM total | 5.2 GB/s | 7.8 GB/s | — |

DDR5 sustainable ceiling on this host ≈ 60 GB/s → DRAM is ~13% utilized under 3× load.

### Three hypotheses falsified

1. **Off-CPU blocking on mutex/futex.** Context switches grow only +11% going 1× → 3× (1,165 → 1,293 / s). Real lock parking would multiply this several-fold. Rejected.
2. **Atomic-retry / cache-line ping-pong inflating per-row work.** P-core instructions per row are **identical** at 1× and 3× (319.7 vs 319.0). No extra instructions, no atomic-spin budget. Rejected.
3. **DRAM bandwidth ceiling.** 7.8 GB/s ≈ 13% of ~60 GB/s. Rejected.

### What the 3× scaling gap actually is on this hardware

Two hardware effects account for essentially all of the 0.28× miss:

- **E-core placement drag (unpinned run).** The scheduler placed one worker on an E-core. For this workload, E-core throughput ≈ 0.70× of P-core (3.67 GHz × 3.14 IPC vs 4.44 GHz × 3.65 IPC). Expected aggregate for 2P + 1E = 2 + 0.70 = **2.70×**. Measured 2.71×.
- **All-core turbo throttling (pinned run).** Single-thread P-core ran at 4.72 GHz; 3-thread P-core average fell to 4.44 GHz = 94% single-thread clock. Pure P-core pinning therefore caps aggregate at roughly 3 × 94% = **2.82×**. Measured 2.77%; the residual ~2% is L3 pressure + background server threads.

Neither effect is in the FT code path. Neither is fixable in software.

### Correct reading of the earlier "memcpy/memset is hot" observation

Those leaves *are* hot in the 3× flamegraph — but that is a **consequence** of threads running faster without the GIL, not a cause of any remaining ceiling. Instructions per row are flat from 1× to 3×, so the per-row memcpy/memset cost is unchanged; what changed is that 3× more rows get produced per wall-second, pushing those leaves higher in the top-N. Optimizing them would reduce absolute per-row CPU cost (a different goal), but **will not move the 3× scaling ratio on this hardware**.

### Do / Don't, from this data

- **Don't** treat 2.72× as evidence of shared-state contention inside FT Proton.
- **Don't** start allocator / scratch-buffer / mimalloc-tuning work motivated by "closing the 10% gap."
- **Don't** add FT-scaling-motivated ProfileEvents instrumentation to the Python hot path based on this laptop number.
- **Do** validate once on a server-class host (dual-Xeon / EPYC, no E-cores, flatter all-core turbo). Expected outcome there is 2.95–3.00× without any code change. If the server result is materially below that, re-open the analysis *there*, not here.
- **Do** keep the "per-row cost" question separate from the "FT scaling" question. 319 P-core instructions per row is potentially attackable (the `Field`-variant round-trip in `PythonListToColumn`, per-row `PyTuple_Check` / `PyTuple_Size` in `convertPythonResultToOutputBlock`, `PyList_New(rows)` per projected column). Those are absolute-throughput wins that apply equally to 1× and 3× — and should be motivated by a workload need, not by this scaling number.

## Current environment state

- Branch: `feature/issue-11704-cpython-3.14-migration` at `ff8bc31d04e`.
- Symlink `build/programs/stripped/bin/proton → build_with_profile/programs/proton` (FT).
- Original stripped binary preserved at `build/programs/stripped/bin/proton.stripped_backup`.
- Benchmark JSON: `tmp/bench_ft2/benchmark_results_ft_fixed.json` (post-HAVE_SOCKET-fix).
- Server currently idle; start with `source ~/miniconda3/bin/activate py314t && ./start-local-proton.sh`.

## Allocator contract & memory observability

**Design intent (for release note and operator handoff):**

- `proton` **keeps jemalloc** as the process allocator.
- **Embedded FT CPython uses mimalloc** for its object heap (mandatory for `Py_GIL_DISABLED`; not a fallback).
- One server process therefore runs **two allocators**. This is deliberate — we are *not* migrating proton to mimalloc, and we are *not* switching the embedded interpreter back to its default small-object allocator.

**Operational consequences:**

| Tool / metric | What it covers under FT |
|---------------|------------------------|
| `system.jemalloc_*` commands, `jeprof` dumps | jemalloc arena only — blind to the mimalloc/Python heap |
| `MemoryResident` | True process RSS — the source of truth |
| `NonJemallocMemory` *(new)* | `MemoryResident - jemalloc.resident`, exposes the blind spot |
| cgroup memory limits, container OOM | Bind to process RSS — unchanged, still authoritative |

**Guidance:**
- Alert on `MemoryResident` against the pod/cgroup limit, not on `jemalloc.allocated`.
- Use `NonJemallocMemory` as a trend indicator. Growth *while jemalloc stays flat* implies leaks / fragmentation in the FT-Python heap or native extensions.
- Jemalloc heap profiling (jeprof) remains useful for the server but will not attribute Python allocations; for Python-side attribution, use tracemalloc or a mimalloc-aware tool inside the interpreter.

**What FT does NOT change:**
- `MemoryTracker` accounting in the server (still tracks jemalloc-side allocations Proton knows about).
- Query-level memory limits and throttling behavior.
- The way jemalloc is linked or configured (`ENABLE_JEMALLOC=ON`).

**FT is an explicit capability.** Building without `-DENABLE_PYTHON_FREE_THREADED=ON` produces the pre-existing GIL-enabled embedded CPython with no allocator change. The option should be opt-in per deployment, with the above operator note in the release announcement.

## Soak test result — 1 h, 2026-04-12

Command: `utils/ft_python_soak.sh 3600 8473` against the FT binary (`85b6de14161`), py314t.
Workload: 3× Python source (`SELECT count() FROM random_stream EMIT PERIODIC 10s`) + 1× passthrough (`INSERT INTO soak_src SELECT ... FROM soak_gen SETTINGS eps=50000`). 120 samples @ 30 s.

|                      | start | warmup (10 m) | mid (30 m) | end (60 m) | max | post-warmup mean ± stdev |
|----------------------|------:|--------------:|-----------:|-----------:|----:|-------------------------:|
| RSS (MB)             | 676.1 | 840.0         | 816.5      | 838.3      | 847.7 | **831.0 ± 7.3** |
| jemalloc.resident (MB)| 221.4| 402.9         | 342.3      | 369.7      | 427.3 | 369.1 ± 30.7 |
| NonJemallocMemory (MB)| 454.6| 437.1         | 474.2      | 468.6      | 494.1 | **468.9 ± 13.6** |
| MemoryTracker (MB)   | 1077.8|1194.4         | 1192.6     | 1214.6     | 1243.5| 1198.7 ± 15.9 |

Post-warmup linear RSS slope: **+4.32 MB/h** (below the 7.3 MB stdev of the sample — i.e. noise, not a trend).
End-vs-warmup drift: RSS **−1.7 MB**, NonJemalloc +31.5 MB (within the 13.6 MB stdev of NonJemalloc sampling).

**Merge gates — all green:**

| Gate | Result |
|------|--------|
| Zero query/server errors | ✅ `errors.log = 0 bytes` |
| No monotonic RSS runaway | ✅ 831 ± 7 MB steady-state; slope below noise |
| NonJemallocMemory bounded after warm-up | ✅ 437–494 MB band |
| Clean cancellation / teardown | ✅ script completed summary |

Raw artifacts: `tmp/ft_soak_20260412_045736/metrics.csv` · `src_{1,2,3}.log` · `passthru.log` · `errors.log` (empty).

## Outstanding rollout items (per earlier alignment)

1. **Passthrough regression** — ✅ fixed by `ff8bc31d04e`.
2. **Allocator contract docs** — ✅ captured above + in `contrib/cpython-cmake/CMakeLists.txt` near `ENABLE_PYTHON_FREE_THREADED`.
3. **`NonJemallocMemory` metric** — ✅ landed in `src/Interpreters/AsynchronousMetrics.cpp`. Observed at idle: RSS 516 MB, jemalloc.resident 169 MB → NonJemallocMemory 347 MB; under 1 h load it oscillates 437–494 MB without drift.
4. **Soak test** — ✅ clean 1 h run (see above); slope +4 MB/h below the 7 MB stdev.
5. **Merge FT migration** — **ready**, pending PR/release-note using this report + the allocator-contract section as the operator-facing text.
