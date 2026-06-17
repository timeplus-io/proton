# FT CPython perf notes

This directory is the home for free-threaded CPython performance reports
and reproducible measurement scripts. Everything here is **documentation**
and **tooling** — raw captures (perf.data, flamegraphs, full stat logs)
stay under `tmp/` and are not checked in.

## Contents

| file | what it is |
|---|---|
| [`REPORT_FT_vs_GIL.md`](REPORT_FT_vs_GIL.md) | FT CPython 3.14 vs GIL CPython 3.10 bench report. Includes throughput, hotspot shift, allocator contract, 1 h soak, and the 2026-04-12 follow-up that attributes the 2.72× ceiling to hybrid-CPU topology + turbo throttling rather than an FT-code bottleneck. |
| [`probe_scaling.sh`](probe_scaling.sh) | Reproducible 1× vs 3× scaling measurement. Validates whether the laptop ceiling holds on a given host. |

## Running `probe_scaling.sh`

```bash
# 1. start an FT build of proton (any port, default 8463)
source ~/miniconda3/bin/activate py314t    # or whatever activates free-threaded CPython
./start-local-proton.sh

# 2. run the probe
src/CPython/perf/probe_scaling.sh
```

Defaults: `--port 8463 --parallel 3 --duration 20 --window 10`.
Skip the pinned run with `--skip-pinned`; redirect artifacts with `--out <dir>`.

### What counts as green

| environment | expected 3× / 1× |
|---|---:|
| server-class CPU (dual Xeon / EPYC, no E-cores, flat all-core turbo) | 2.95 – 3.00× |
| hybrid laptop CPU (Alder Lake / Meteor Lake P+E) | 2.65 – 2.85× (unpinned), 2.75 – 2.85× (P-core pinned) |

Also green on any hardware:

- **P-core instructions per row unchanged** between 1× and 3× (no atomic-retry contention)
- **ctxsw/s growth below 2×** going 1× → 3× (no mutex-blocking contention)
- **DRAM under 30% of sustainable ceiling** (no bandwidth ceiling)

If a server-class host lands below ~2.9×, the FT scaling analysis needs to
re-open *there*. Don't re-open it from laptop numbers — the report explains why.

## Relationship to the main bench

The main FT vs GIL benchmark is `tmp/bench_now/bench.py` (from issue #11797).
`probe_scaling.sh` reproduces only the Py→C++ source phase because that's the
path where the FT migration shows an architectural win and where the scaling
question lives. For full-matrix results (sink, passthrough, data-type breakdown),
run `bench.py` directly.
