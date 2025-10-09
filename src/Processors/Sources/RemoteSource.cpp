#include <Processors/Sources/RemoteSource.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <QueryPipeline/RemoteQueryExecutorReadContext.h>
#include <QueryPipeline/StreamLocalLimits.h>
#include <Processors/Transforms/convertToChunk.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Common/Exception.h>

/// proton: starts.
#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/CheckpointCoordinator.h>
/// proton: ends.

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

RemoteSource::RemoteSource(RemoteQueryExecutorPtr executor, bool add_aggregation_info_, bool async_read_, UUID uuid_, bool is_streaming_)
    : Streaming::ISource(executor->getHeader(), false, getLogger("RemoteSource"), ProcessorID::RemoteSourceID)
    , empty_chunk({executor->getHeader().getColumns(), 0}) /// proton: added
    , add_aggregation_info(add_aggregation_info_)
    , query_executor(std::move(executor))
    , async_read(async_read_)
    , uuid(uuid_)
{
    /// Add AggregatedChunkInfo if we expect DataTypeAggregateFunction as a result.
    const auto & sample = getPort().getHeader();
    for (auto & type : sample.getDataTypes())
        if (typeid_cast<const DataTypeAggregateFunction *>(type.get()))
            add_aggregation_info = true;

    /// proton: starts
    setStreaming(is_streaming_);

    timeout_ms = is_streaming_ ? query_executor->getContext()->getSettingsRef().record_consume_timeout_ms.value : 0;
    /// proton: ends
}

RemoteSource::~RemoteSource() = default;

void RemoteSource::connectToScheduler(InputPort & input_port)
{
    outputs.emplace_back(Block{}, this);
    dependency_port = &outputs.back();
    connect(*dependency_port, input_port);
}

UUID RemoteSource::getParallelReplicasGroupUUID()
{
    return uuid;
}

void RemoteSource::setStorageLimits(const std::shared_ptr<const StorageLimitsList> & storage_limits_)
{
    /// Remove leaf limits for remote source.
    StorageLimitsList list;
    for (const auto & value : *storage_limits_)
        list.emplace_back(StorageLimits{value.local_limits, {}});

    storage_limits = std::make_shared<const StorageLimitsList>(std::move(list));
}

ISource::Status RemoteSource::prepare()
{
    /// Check if query was cancelled before returning Async status. Otherwise it may lead to infinite loop.
    if (was_query_canceled)
    {
        getPort().finish();
        return Status::Finished;
    }

    if (is_async_state)
        return Status::Async;

    Status status = ISource::prepare();
    /// To avoid resetting the connection (because of "unfinished" query) in the
    /// RemoteQueryExecutor it should be finished explicitly.
    if (status == Status::Finished)
    {
        query_executor->finish(&read_context);
        if (dependency_port)
            dependency_port->finish();
        is_async_state = false;

        return status;
    }

    if (status == Status::PortFull)
    {
        /// Also push empty chunk to dependency to signal that we read data from remote source
        /// or answered to the incoming request from parallel replica
        if (dependency_port && !dependency_port->isFinished() && dependency_port->canPush())
            dependency_port->push(Chunk());
    }

    return status;
}

Chunk RemoteSource::generate()
{
    /// onCancel() will do the cancel if the query was sent.
    if (was_query_canceled)
        return {};

    if (!was_query_sent)
    {
        /// Progress method will be called on Progress packet.
        query_executor->setProgressCallback([this](const Progress & value)
        {
            if (value.total_rows_to_read)
                addTotalRowsApprox(value.total_rows_to_read);
            if (value.total_bytes_to_read)
                addTotalBytes(value.total_bytes_to_read);
            progress(value.read_rows, value.read_bytes);
        });

        /// Get rows_before_limit result for remote query from ProfileInfo packet.
        query_executor->setProfileInfoCallback([this](const ProfileInfo & info)
        {
            if (rows_before_limit && info.hasAppliedLimit())
                rows_before_limit->set(info.getRowsBeforeLimit());
        });

        query_executor->sendQuery();

        was_query_sent = true;
    }

    Block block;

    if (async_read)
    {
        auto res = query_executor->read(read_context, timeout_ms);

        if (res.getType() == RemoteQueryExecutor::ReadResult::Type::Nothing)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Got an empty packet from the RemoteQueryExecutor. This is a bug");

        if (res.getType() == RemoteQueryExecutor::ReadResult::Type::FileDescriptor)
        {
            fd = res.getFileDescriptor();
            is_async_state = true;
            return empty_chunk.clone(); /// proton: updated (required by Streaming::ISource)
        }

        if (res.getType() == RemoteQueryExecutor::ReadResult::Type::ParallelReplicasToken)
        {
            is_async_state = false;
            return empty_chunk.clone(); /// proton: updated (required by Streaming::ISource)
        }

        is_async_state = false;

        block = res.getBlock();
    }
    else
        block = query_executor->readBlock(timeout_ms);

    if (!block)
    {
        query_executor->finish(&read_context);
        return {};
    }

    UInt64 num_rows = block.rows();
    Chunk chunk(block.getColumns(), num_rows);

    if (add_aggregation_info)
    {
        auto info = std::make_shared<AggregatedChunkInfo>();
        info->bucket_num = block.info.bucket_num;
        info->is_overflows = block.info.is_overflows;
        chunk.setChunkInfo(std::move(info));
    }

    /// proton: starts
    if (block.hasWatermark())
        chunk.setWatermark(block.watermark());

    if (block.hasSN()) /// Only exists in remote fetch columns scenario
    {
        if (isStreaming())
            setLastProcessedSN(block.getSN());
        else
            /// In non-streaming scenarios, the Sequence Number (SN) indicate the maximum sequence number
            /// of historical data for backfill reads, which is used in Streaming::ConcatProcessor to reset start sn of streaming source.
            chunk.setSN(block.getSN());
    }
    /// proton: ends

    return chunk;
}

void RemoteSource::onCancel() noexcept
{
    try
    {
        query_executor->cancel();
    }
    catch (...)
    {
        tryLogCurrentException(getLogger("RemoteSource"), "Error occurs on cancellation.");
    }
}

void RemoteSource::onUpdatePorts()
{
    if (getPort().isFinished())
    {
        was_query_canceled = true;
        query_executor->finish(&read_context);
        // is_async_state = false;
    }
}

/// proton: starts.
Chunk RemoteSource::doCheckpoint(CheckpointContextPtr ckpt_ctx_)
{
    /// Prepare checkpoint barrier chunk
    auto result = Chunk{getPort().getHeader().getColumns(), 0};
    result.setCheckpointContext(ckpt_ctx_);

    ckpt_ctx_->coordinator->checkpoint(getVersion(), getLogicID(), ckpt_ctx_, [&](WriteBuffer & wb) {
        writeIntBinary(lastProcessedSN(), wb);
    });

    LOG_INFO(logger, "Saved checkpoint sn={}", lastProcessedSN());
    return result;
}

void RemoteSource::doRecover(CheckpointContextPtr ckpt_ctx_)
{
    ckpt_ctx_->coordinator->recover(getLogicID(), ckpt_ctx_, [&](VersionType /*version*/, ReadBuffer & rb) {
        Int64 recovered_last_sn = 0;
        readIntBinary(recovered_last_sn, rb);
        setLastCheckpointSN(recovered_last_sn);
    });

    LOG_INFO(logger, "Recovered last_sn={}", lastCheckpointSN());
}

void RemoteSource::doResetStartSN(Int64 sn)
{
    if (sn >= cluster::Constants::LogStartSN)
        query_executor->seekTo(sn);
}
/// proton: ends.

RemoteTotalsSource::RemoteTotalsSource(RemoteQueryExecutorPtr executor)
    : ISource(executor->getHeader(), true, ProcessorID::RemoteTotalsSourceID)
    , query_executor(std::move(executor))
{
}

RemoteTotalsSource::~RemoteTotalsSource() = default;

Chunk RemoteTotalsSource::generate()
{
    if (auto block = query_executor->getTotals())
    {
        UInt64 num_rows = block.rows();
        return Chunk(block.getColumns(), num_rows);
    }

    return {};
}


RemoteExtremesSource::RemoteExtremesSource(RemoteQueryExecutorPtr executor)
    : ISource(executor->getHeader(), true, ProcessorID::RemoteExtremesSourceID)
    , query_executor(std::move(executor))
{
}

RemoteExtremesSource::~RemoteExtremesSource() = default;

Chunk RemoteExtremesSource::generate()
{
    if (auto block = query_executor->getExtremes())
    {
        UInt64 num_rows = block.rows();
        return Chunk(block.getColumns(), num_rows);
    }

    return {};
}


Pipe createRemoteSourcePipe(
    RemoteQueryExecutorPtr query_executor,
    bool add_aggregation_info, bool add_totals, bool add_extremes, bool async_read, UUID uuid, bool is_streaming)
{
    Pipe pipe(std::make_shared<RemoteSource>(query_executor, add_aggregation_info, async_read, uuid/* proton: starts */, is_streaming/* proton: ends */));

    if (add_totals)
        pipe.addTotalsSource(std::make_shared<RemoteTotalsSource>(query_executor));

    if (add_extremes)
        pipe.addExtremesSource(std::make_shared<RemoteExtremesSource>(query_executor));

    return pipe;
}

}
