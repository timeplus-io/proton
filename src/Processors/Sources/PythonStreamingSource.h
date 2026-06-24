#pragma once

#include "config.h"

#if USE_PYTHON_UDF

#include <CPython/PythonModuleSession.h>
#include <Common/CurrentMetrics.h>
#include <Processors/Streaming/ISource.h>

#include <mutex>

namespace CurrentMetrics
{
extern const Metric PythonLivePipelines;
}


namespace DB
{
/// PythonStreamingSource reads data from a Python generator/iterator
/// and emits chunks incrementally as they are yielded.
///
/// Thread-safety contract (free-threading ready):
///   - generate() is called by a single pipeline worker thread at a time.
///   - onCancel() may be called concurrently from a different thread.
///   - finishPython() may be called from generate(), onCancel(), or the
///     destructor — guarded by `finish_once` to prevent double execution.
///   - Under free-threading the GIL guard no longer serializes Python access,
///     so `py_obj_mutex` guards the *lifetime* of the `py_iterator` member:
///     readers (generate()/onCancel()) take a strong ref under the lock and
///     release it before any Python call, while finishPython() detaches the
///     member under the lock. The mutex protects the C++ pointer move/reset
///     lifetime only; Python calls run without holding it, so a blocked
///     iterator can still be interrupted by onCancel().
class PythonStreamingSource final : public Streaming::ISource
{
public:
    PythonStreamingSource(
        Block header, cpython::PyObjectPtr py_iterator_, DataTypePtr tuple_type_, cpython::PythonModuleSessionPtr session_);

    ~PythonStreamingSource() override;

    String getName() const override { return "PythonStreamingSource"; }

protected:
    void onCancel() noexcept override;

    Chunk generate() override;

    /// Non-replayable source: a Python generator cannot be seeked, so there is
    /// no resumable position to persist or restore. doCheckpoint emits a barrier
    /// and notifies the coordinator but saves no recoverable SN; recovery and
    /// start-SN reset are no-ops (mirrors GenerateRandomSource). This avoids
    /// advertising a recovered offset the generator cannot honor, which would
    /// otherwise replay already-emitted rows into recovered downstream state.
    Chunk doCheckpoint(CheckpointContextPtr) override;
    void doRecover(CheckpointContextPtr) override { }
    void doResetStartSN(Int64 /*sn*/) override { }

private:
    Block convertPythonResultToBlock(const cpython::PyObjectPtr & py_result) const;

    Block convertPythonResultToOutputBlock(const cpython::PyObjectPtr & py_result) const;

    void init();
    void finishPython(bool ignore_exceptions, bool acquire_gil = true);

    cpython::PyObjectPtr py_iterator;
    DataTypePtr tuple_type;
    cpython::PythonModuleSessionPtr session;

    std::vector<size_t> output_tuple_positions;
    bool needs_projection_pushdown = false;

    /// --- Concurrency state ---------------------------------------------------
    /// `exhausted` is read/written from generate() and onCancel()/finishPython()
    /// which may run on different threads.
    std::atomic_bool exhausted{false};

    /// Thread ID of the thread currently executing Python inside generate().
    /// Written by generate() (release), read by onCancel() (acquire).
    /// 0 means no thread is currently inside Python.
    std::atomic<unsigned long> python_thread_id{0};

    std::atomic_bool cancel_requested{false};

    /// Guards the lifetime (move/reset) of the `py_iterator` member against
    /// concurrent access from generate() / onCancel() / finishPython().
    /// Never held across a Python call — see the class contract above.
    std::mutex py_obj_mutex;

    /// Guards finishPython() against double execution from concurrent
    /// generate() + destructor or onCancel() + destructor.
    std::once_flag finish_once;

    CurrentMetrics::Increment live_pipeline_count{CurrentMetrics::PythonLivePipelines};
};
}

#endif
