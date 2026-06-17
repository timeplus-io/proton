#pragma once

#include <cstddef>
#include <cstdint>

namespace DB::cpython
{

/// Snapshot of mimalloc-owned memory, sourced from mi_process_info().
///
/// On free-threaded CPython builds mimalloc is pinned as the object allocator
/// (required by Py_GIL_DISABLED). These counters let operators see that heap
/// directly instead of inferring it from `MemoryResident - jemalloc.resident`.
///
/// Only honest mimalloc-tracked fields are kept. mi_process_info() also fills
/// current_rss and peak_rss, but on Linux current_rss is initialised to equal
/// current_commit (mimalloc's own counter, not process RSS) and peak_rss is
/// overwritten from getrusage — duplicating the existing `MemoryResidentMax`
/// async metric. Dropping them keeps each metric name meaning what it says.
struct MimallocProcessInfo
{
    uint64_t current_commit = 0;
    uint64_t peak_commit = 0;
};

/// Read mimalloc process-wide stats. Thread-safe; reads allocator-maintained
/// atomic counters and does not touch `PyThreadState`, so no GIL guard is
/// needed. Returns a zeroed struct when mimalloc is not linked in (i.e.
/// when the embedded CPython was built without `Py_GIL_DISABLED`, or when
/// Python UDF support is disabled entirely).
MimallocProcessInfo getMimallocProcessInfo();

/// True when the embedded CPython was built with WITH_MIMALLOC, which matches
/// the ENABLE_PYTHON_FREE_THREADED=ON build path. Lets async-metrics
/// registration skip the mimalloc-specific metrics on GIL builds.
bool mimallocEnabled();

}
