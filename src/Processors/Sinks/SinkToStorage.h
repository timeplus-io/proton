#pragma once
#include <Storages/TableLockHolder.h>
#include <Processors/Transforms/ExceptionKeepingTransform.h>

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

protected:
    virtual void consume(Chunk chunk) = 0;
    virtual bool lastBlockIsDuplicate() const { return false; }

private:
    std::vector<TableLockHolder> table_locks;

    void onConsume(Chunk chunk) override;
    GenerateResult onGenerate() override;

    Chunk cur_chunk;
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
