#pragma once

#include <Core/BlockWithShard.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Streaming/SizedChunkSplitter.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/Stream/IngestMode.h>

#include <Cluster/Common/CallResult.h>
#include <Cluster/SchemaRecord/SchemaRecord.h>

#include <Common/Logger.h>

#include <atomic>
#include <vector>

namespace DB
{

struct Settings;
class IStorage;

class StreamSinkBase : public SinkToStorage
{
public:
    StreamSinkBase(
        StoragePtr storage_,
        StorageMetadataPtr metadata_snapshot_,
        IngestMode ingest_mode_,
        UInt32 shards_,
        ContextPtr query_context_,
        LoggerPtr logger_,
        ProcessorID pid_);

    void consume(Chunk chunk) override;
    void onFinish() override;
    void checkpoint(CheckpointContextPtr ckpt_ctx) override;

protected:
    static UInt64 getMaxInsertBlockRows(const Settings & settings);
    static UInt64 getMaxInsertBlockBytes(const Settings & settings, const IStorage & storage);
    static bool isErrorRetryable(int error_code);
    static std::string getBlockIdempotentKey(const std::string & idempotent_key, UInt64 chunk_id_, size_t block_id_);

    virtual BlocksWithShard shardBlock(Block block) const = 0;
    virtual cluster::CallResultV<int64_t> appendLog(cluster::SchemaRecordPtr & record, int64_t timeout_ms) = 0;

protected:
    StoragePtr storage;
    StorageMetadataPtr metadata_snapshot;
    ContextPtr query_context;
    IngestMode ingest_mode;
    UInt32 shards;

    std::pair<UInt16, std::vector<UInt16>> versioned_column_positions{0, {}};

    int64_t append_timeout_ms;
    uint64_t max_retries;
    uint64_t retry_initial_backoff_ms;
    uint64_t retry_max_backoff_ms;

    struct IngestState
    {
        bool hasError() const noexcept { return last_errcode != 0; }
        bool hasOutstandingBlocks() const noexcept { return committed_blocks + failed_blocks < total_blocks; }

        std::atomic_uint64_t total_blocks = 0;
        std::atomic_uint64_t committed_blocks = 0;
        std::atomic_uint64_t failed_blocks = 0;

        std::atomic_int32_t last_errcode = 0;
    };

    Streaming::SizedChunkSplitter chunk_splitter;

    /// Track the consume chunk index for generating unique idempotent key
    UInt64 chunk_id = 0;
    IngestState ingest_state;

    LoggerPtr logger;

private:
    const std::vector<UInt16> & getColumnPositions(UInt16 version_requested);
    void appendWithRetry(cluster::SchemaRecordPtr & record);
};

}
