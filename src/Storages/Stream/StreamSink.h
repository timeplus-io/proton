#pragma once

#include <Storages/StreamSinkBase.h>

namespace DB
{
class StorageStream;

class StreamSink final : public StreamSinkBase
{
public:
    StreamSink(std::shared_ptr<StorageStream> storage_, StorageMetadataPtr metadata_snapshot_, ContextPtr query_context_);
    ~StreamSink() override = default;

    String getName() const override { return "StreamSink"; }

private:
    BlocksWithShard shardBlock(Block block) const override;
    cluster::CallResultV<int64_t> appendLog(cluster::SchemaRecordPtr & record, int64_t timeout_ms) override;

private:
    StorageStream * storage_stream;
};

}

