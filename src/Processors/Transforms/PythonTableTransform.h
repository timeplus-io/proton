#pragma once

#include "config.h"

#if USE_PYTHON_UDF

#include <CPython/PythonModuleSession.h>
#include <Common/CurrentMetrics.h>
#include <Processors/ISimpleTransform.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ExternalStream/Python/StoragePythonTable.h>

#include <atomic>
#include <memory>
#include <mutex>

namespace CurrentMetrics
{
extern const Metric PythonLivePipelines;
}

namespace DB
{
/// Shared lifecycle for a PythonModuleSession held by multiple parallel
/// PythonTableTransforms. The last transform to finalize via work() claims
/// the close and runs deinit with the caller's ignore_exceptions semantics;
/// earlier finalizers only decrement, and abnormal exits decrement quietly
/// in the destructor without claiming. Set to nullptr on the transform for
/// the single-owner case (e.g. unit tests), where the transform itself is
/// always "last".
struct PythonTableTransformSharedState
{
    cpython::PythonModuleSessionPtr session;
    std::atomic<size_t> live_count{0};

    void retain() noexcept { live_count.fetch_add(1, std::memory_order_relaxed); }

    /// Returns true iff this caller is the last live owner.
    bool releaseAndClaimLast() noexcept
    {
        return live_count.fetch_sub(1, std::memory_order_acq_rel) == 1;
    }
};
using PythonTableTransformSharedStatePtr = std::shared_ptr<PythonTableTransformSharedState>;

/// PythonTableTransform applies a Python external stream function per input chunk.
///
/// Thread-safety contract (free-threading ready):
///   - prepare(), work(), transform() are called by a single pipeline thread.
///   - onCancel() may be called concurrently from a different thread.
///   - When PythonModuleSession is shared across parallel transforms (storage
///     readImpl via addSimpleTransform), pass a non-null
///     PythonTableTransformSharedStatePtr; only the last transform to finalize
///     via work() runs the explicit close. Other transforms decrement the
///     counter and skip the close to avoid tearing down the module while
///     siblings are still executing. Abnormal exits release the counter in
///     the destructor without claiming; ~PythonModuleSession is the final
///     safety net (ignore_exceptions=true).
///   - When shared_state is null (single-owner construction, e.g. unit tests),
///     the transform is always the sole owner and finalization closes the
///     session directly with the caller's ignore_exceptions semantics.
class PythonTableTransform final : public ISimpleTransform
{
public:
    PythonTableTransform(
        const Block & input_header,
        Block output_header,
        Names input_column_names_,
        ColumnsDescription output_columns_,
        Names requested_output_columns_,
        PythonTableMode mode_,
        cpython::PythonModuleSessionPtr session_,
        PythonTableTransformSharedStatePtr shared_state_ = nullptr);

    ~PythonTableTransform() override;

    String getName() const override { return "PythonTableTransform"; }

    Status prepare() override;
    void work() override;

protected:
    void onCancel() noexcept override;

    void transform(Chunk & chunk) override;

private:
    Block convertPythonResultToBlock(const cpython::PyObjectPtr & py_result) const;
    Block filterOutputBlock(Block && block) const;

    void finishSession(bool ignore_exceptions);

    std::vector<size_t> input_positions;
    Names requested_output_columns;
    ColumnsDescription output_columns;
    DataTypePtr tuple_type;
    PythonTableMode mode;
    cpython::PythonModuleSessionPtr session;
    PythonTableTransformSharedStatePtr shared_state;

    /// --- Concurrency state ---------------------------------------------------
    /// Thread ID of the thread currently executing Python inside transform().
    /// Written by transform() (release), read by onCancel() (acquire).
    std::atomic<unsigned long> python_thread_id{0};
    std::atomic_bool cancel_requested{false};

    bool finalization_pending = false;
    bool finalization_completed = false;
    bool finalization_ignore_exceptions = true;

    /// Guards explicit close against double execution from work() + destructor.
    std::once_flag finish_once;

    CurrentMetrics::Increment live_pipeline_count{CurrentMetrics::PythonLivePipelines};
};
}

#endif
