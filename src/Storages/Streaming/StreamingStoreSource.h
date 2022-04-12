#pragma once

#include "StreamingStoreSourceBase.h"

namespace DB
{
class StreamingBlockReaderKafka;
class StreamingBlockReaderNativeLog;

class StreamingStoreSource final : public StreamingStoreSourceBase
{
public:
    StreamingStoreSource(
        std::shared_ptr<IStorage> storage_,
        const Block & header,
        const StorageMetadataPtr & metadata_snapshot_,
        ContextPtr context_,
        Int32 shard_,
        Int64 sn,
        Poco::Logger * log_);

    ~StreamingStoreSource() override = default;

    String getName() const override { return "StreamingStoreSource"; }

private:
    inline nlog::RecordPtrs read();
    void readAndProcess() override;

private:
    Int32 shard;
    Poco::Logger * log;

    std::unique_ptr<StreamingBlockReaderKafka> kafka_reader;

    std::unique_ptr<StreamingBlockReaderNativeLog> nativelog_reader;

    UInt64 record_consume_batch_count = 1000;
    Int64 record_consume_timeout = 100;
};

using StreamingStoreSourcePtr = std::shared_ptr<StreamingStoreSource>;
}
