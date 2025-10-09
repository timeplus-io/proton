#pragma once
#include <IO/Progress.h>
#include <Processors/Transforms/ExceptionKeepingTransform.h>
#include <Storages/TableLockHolder.h>

namespace DB
{

/// Sink which is returned from Storage::write.
class SinkToStorage : public ExceptionKeepingTransform
{
/// PartitionedSink owns nested sinks.
friend class PartitionedSink;

public:
    explicit SinkToStorage(const Block & header, ProcessorID pid_);

    const Block & getHeader() const { return inputs.front().getHeader(); }
    void addTableLock(const TableLockHolder & lock) { table_locks.push_back(lock); }

    /// proton: starts.
    void setProgressCallback(ProgressCallback callback) { progress_callback.swap(callback); }
    /// proton: ends

protected:
    virtual void consume(Chunk chunk) = 0;
    virtual bool lastBlockIsDuplicate() const { return false; }

    /// proton: starts
    static bool isErrorRetryable(int error_code);
    /// proton: ends

private:
    std::vector<TableLockHolder> table_locks;

    void onConsume(Chunk chunk) override;
    GenerateResult onGenerate() override;

    Chunk cur_chunk;

    /// proton: starts.
    ProgressCallback progress_callback;
    /// proton: ends.
};

using SinkToStoragePtr = std::shared_ptr<SinkToStorage>;


class NullSinkToStorage final : public SinkToStorage
{
public:
    explicit NullSinkToStorage(const Block & header) : SinkToStorage(header, ProcessorID::NullSinkToStorageID) { }
    std::string getName() const override { return "NullSinkToStorage"; }
    void consume(Chunk) override {}
};

using SinkPtr = std::shared_ptr<SinkToStorage>;
}
