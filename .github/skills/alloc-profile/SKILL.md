---
name: alloc-profile
description: Analyze jemalloc or async-profiler allocation profiles in collapsed stack format. Use when the user wants top allocators, memory hotspots, or profile interpretation from a .collapsed or .folded file.
---

# Allocation Profile

## Inputs

- `$0` (optional): path to a `.collapsed` or `.folded` profile

If no path is given, search near the current directory:

```bash
find . -maxdepth 4 \( -name "*.collapsed" -o -name "*.folded" \) | sort
```

## Tooling

Use the bundled analyzer script:

```bash
python3 .github/skills/alloc-profile/scripts/analyze_alloc_profile.py <profile>
```

## What the analyzer reports

- Total samples/bytes and unique stack traces
- Top stack traces
- Top outermost meaningful frames
- Top leaf allocation frames

This gives both:

- "why did we allocate?" via outermost frames
- "what code actually allocated?" via leaf frames

## Interpretation guidance

- Large outermost buckets often point to the higher-level operation to optimize first.
- Large leaf buckets inside allocators or containers are usually symptoms; look one or two frames above for product code.
- For streaming paths, pay special attention to joins, windows, aggregation state, and block materialization.
- For historical paths, focus on scans, merges, deserialization, and sort/aggregation buffers.

## Related repo context

- Jemalloc profile flushing config lives in [tests/config/config.d/jemalloc_flush_profile.yaml](../../../tests/config/config.d/jemalloc_flush_profile.yaml).
- Existing profiling notes live in [tests/instructions/jemalloc_memory_profile.txt](../../../tests/instructions/jemalloc_memory_profile.txt).

## Output expectations

Summarize:

1. biggest operations by memory share
2. likely owning subsystem
3. likely next inspection target in code
4. whether the profile looks like steady-state memory, burst allocation, or leak-like retention
