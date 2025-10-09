#include <Storages/ExternalStream/Pulsar/PulsarSource.h>

#if USE_PULSAR

#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/CheckpointCoordinator.h>
#include <Interpreters/Context.h>
#include <Storages/ExternalStream/Pulsar/Pulsar.h>
#include <Storages/ExternalStream/Pulsar/Util.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>

#include <pulsar/Result.h>
#include <Poco/Base64Encoder.h>

#include <sstream>

namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_CONNECT_SERVER;
extern const int CANNOT_RECEIVE_MESSAGE;
extern const int INVALID_SETTING_VALUE;
extern const int RECOVER_CHECKPOINT_FAILED;
}

namespace ExternalStream
{

namespace
{
inline String formatMessageId(const pulsar::MessageId & id)
{
    /// pulsar::MessageID does not have a toString method, instead, it implements `<<` to write its string format to an output stream.
    std::ostringstream os;
    os << id;
    return os.str();
}

Int64 getGenerateTimeoutMs(const ContextPtr & context)
{
    auto timeout_setting = context->getSettingsRef().record_consume_timeout_ms.value;
    if (timeout_setting < 0)
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "record_consume_timeout_ms cannot be smaller than 0");

    return timeout_setting;
}

}

PulsarSource::PulsarSource(
    const Block & header_,
    const StorageSnapshotPtr & storage_snapshot_,
    std::map<size_t, std::pair<DataTypePtr, std::function<Field(const pulsar::Message &)>>> virtual_header_,
    bool is_streaming_,
    const String & data_format,
    const FormatSettings & format_settings,
    pulsar::Reader && reader_,
    const std::shared_ptr<Pulsar> & storage_,
    ExternalStreamCounterPtr counter,
    LoggerPtr logger_,
    const ContextPtr & context_)
    : Streaming::ISource(header_, true, logger_, ProcessorID::PulsarSourceID)
    , ExternalStreamSource(header_, storage_snapshot_, context_->getSettingsRef().max_block_size.value, context_)
    , virtual_header(virtual_header_)
    , generate_timeout_ms(getGenerateTimeoutMs(context_))
    , storage(storage_)
    , reader(std::move(reader_))
    , ignore_format_errors(format_settings.ignore_parsing_errors)
    , external_stream_counter(counter)
{
    setStreaming(is_streaming_);
    if (!is_streaming_)
    {
        auto res = executeWithRetry(
            [&, this]() { return reader.getLastMessageId(end_message_id); }, "getLastMessageId", /*timeout_ms=*/2'000, logger);

        if (res != pulsar::ResultOk)
            throw Exception(
                ErrorCodes::CANNOT_CONNECT_SERVER, "Failed to get last message id of topic {}: {}", getTopic(), pulsar::strResult(res));
    }

    initInputFormatExecutor(data_format, format_settings);

    setDescription(fmt::format("topic={}", getTopic()));
}

PulsarSource::~PulsarSource()
{
    if (!isCancelled())
        onCancel();
}

Chunk PulsarSource::generate()
{
    if (isCancelled() || is_finished)
        return {};

    if (consume_exception)
        consume_exception->rethrow();

    MutableColumns batch;
    pulsar::Message msg;
    size_t rows = 0;
    Int64 consumed_messages = 0;

    auto timeout_ms = generate_timeout_ms;
    Stopwatch stopwatch;
    while (timeout_ms > 0 && rows < max_block_size && !isCancelled() && !is_finished)
    {
        auto res = reader.readNext(msg, static_cast<int>(timeout_ms));

        if (res == pulsar::ResultTimeout || res == pulsar::ResultAlreadyClosed)
        {
            /// For non-streaming queries, if there is no message available, no needs to wait for more messages.
            if (!is_streaming)
                is_finished = true;
            break;
        }

        if (res != pulsar::ResultOk)
            throw Exception(
                ErrorCodes::CANNOT_RECEIVE_MESSAGE, "Failed to receive message from topic {}: {}", getTopic(), pulsar::strResult(res));

        if (!is_streaming && msg.getMessageId() == end_message_id)
            is_finished = true;

        timeout_ms = generate_timeout_ms - static_cast<Int64>(stopwatch.elapsedMilliseconds());
        if (consumed_messages == 0)
        {
            /// reader.seek(msg_id) excludes the msg_id, thus, if latest_consumed_message_id is available, should use it
            if (latest_consumed_message_id)
                latest_consumed_batch_begin_message_id = latest_consumed_message_id;
            else
                latest_consumed_batch_begin_message_id = {msg.getMessageId()};
        }

        ++consumed_messages;

        if (messages_to_skip > 0)
        {
            --messages_to_skip;
            LOG_INFO(
                logger, "Message {} skipped as start SN reset, {} more to skip", formatMessageId(msg.getMessageId()), messages_to_skip);
            continue;
        }

        external_stream_counter->addReadBytes(msg.getLength());

        ReadBufferFromMemory buf(static_cast<const char *>(msg.getData()), msg.getLength());
        size_t new_rows = 0;
        try
        {
            new_rows = format_executor->execute(buf);
        }
        catch (Exception & ex)
        {
            if (ignore_format_errors)
            {
                LOG_ERROR(
                    logger,
                    "Failed to parse message topic={} message_id={} error={}",
                    getTopic(),
                    formatMessageId(msg.getMessageId()),
                    ex.message());
                continue;
            }
            else
            {
                ex.addMessage("Failed to parse message topic={} message_id={}", getTopic(), formatMessageId(msg.getMessageId()));
                consume_exception.emplace(std::move(ex));
                /// Do not read more messages, instead move on and record the lastProcessedSN.
                break;
            }
        }

        auto new_data = format_executor->getResultColumns();

        if (virtual_header.empty())
        {
            if (!batch.empty())
                for (size_t pos = 0; pos < batch.size(); ++pos)
                {
                    batch[pos]->insertRangeFrom(*new_data[pos], 0, new_rows);
                }
            else
                batch = std::move(new_data);
        }
        else
        {
            if (!batch.empty())
            {
                for (size_t pos = 0, i = 0; pos < batch.size(); ++pos)
                {
                    if (!virtual_header.contains(pos))
                        batch[pos]->insertRangeFrom(*new_data[i++], 0, new_rows);
                    else
                        batch[pos]->insertMany(virtual_header[pos].second(msg), new_rows);
                }
            }
            else
            {
                for (size_t pos = 0, i = 0; pos < header_chunk.getNumColumns(); ++pos)
                {
                    if (!virtual_header.contains(pos))
                    {
                        batch.push_back(std::move(new_data[i++]));
                    }
                    else
                    {
                        auto vheader = virtual_header[pos];
                        auto column = vheader.first->createColumn();
                        column->insertMany(vheader.second(msg), new_rows);
                        batch.push_back(std::move(column));
                    }
                }
            }
        }

        rows += new_rows;
    }

    if (consumed_messages > 0)
    {
        auto last_processed_sn = lastProcessedSN();
        setLastProcessedSNRange({.start = last_processed_sn + 1, .end = last_processed_sn + consumed_messages});
        latest_consumed_message_id = msg.getMessageId();
    }

    if (rows != 0u)
    {
        external_stream_counter->addReadRows(rows);
        return {std::move(batch), rows};
    }
    else
        return header_chunk.clone();
}

void PulsarSource::onCancel() noexcept
{
    LOG_INFO(logger, "Cancelling");

    try
    {
        reader.close();
    }
    catch (...)
    {
        tryLogCurrentException(logger, "Failed to close Puslar reader");
    }
}

Chunk PulsarSource::doCheckpoint(CheckpointContextPtr ckpt_ctx_)
{
    /// Prepare checkpoint barrier chunk
    auto result = header_chunk.clone();
    result.setCheckpointContext(ckpt_ctx_);

    ckpt_ctx_->coordinator->checkpoint(getVersion(), getLogicID(), ckpt_ctx_, [&](WriteBuffer & wb) {
        if (latest_consumed_message_id)
        {
            const auto & topic = getTopic();
            writeStringBinary(topic, wb);

            /// Need to get the formatted message ID before calling `serialize`, otherwise will get segment falt.
            auto msg_id_str = formatMessageId(*latest_consumed_message_id);

            String message_id_bytes;
            latest_consumed_message_id->serialize(message_id_bytes);
            writeStringBinary(message_id_bytes, wb);

            auto last_processed_sn = lastProcessedSN();
            writeVarInt(last_processed_sn, wb);

            LOG_INFO(logger, "Saved checkpoint topic={} message_id={} last_processed_sn={}", getTopic(), msg_id_str, lastProcessedSN());
        }
        else
        {
            LOG_INFO(logger, "No message consumed yet, skip doCheckpoint");
        }
    });

    /// Popagate checkpoint barriers
    return result;
}

void PulsarSource::doRecover(CheckpointContextPtr ckpt_ctx_)
{
    ckpt_ctx_->coordinator->recover(getLogicID(), ckpt_ctx_, [&](VersionType, ReadBuffer & rb) {
        if (rb.eof()) /// No checkpoint data
        {
            LOG_INFO(logger, "No checkpoint to recover");
            return;
        }

        String recovered_topic;
        readStringBinary(recovered_topic, rb);

        if (recovered_topic != getTopic())
            throw Exception(
                ErrorCodes::RECOVER_CHECKPOINT_FAILED, "Found mismatched topic. recovered={}, current={}", recovered_topic, getTopic());

        String recovered_message_id_bytes;
        readStringBinary(recovered_message_id_bytes, rb);

        pulsar::MessageId recovered_message_id;
        try
        {
            recovered_message_id = pulsar::MessageId::deserialize(recovered_message_id_bytes);
        }
        catch (std::invalid_argument &)
        {
            std::ostringstream error;
            error << "Failed to parse message ID from checkpoint data (base64 encoded): ";
            Poco::Base64Encoder enc(error);
            enc << recovered_message_id_bytes;

            throw Exception(ErrorCodes::RECOVER_CHECKPOINT_FAILED, "{}", error.str());
        }

        Int64 recovered_sn;
        readVarInt(recovered_sn, rb);
        setLastCheckpointSN(recovered_sn);

        // If lastProcessedSN > -1, it means it's now in auto-recovery phase, and it needs to
        // skip some bad data (which causes exceptions). Otherwise, for a normal recovery,
        // lastProcessedSN should be -1 at this point.
        if (auto sn = lastProcessedSN(); sn >= 0)
            messages_to_skip = sn - recovered_sn;

        LOG_INFO(
            logger, "Recovered checkpoint topic={} message_id={} sn={}", getTopic(), formatMessageId(recovered_message_id), recovered_sn);

        auto res = executeWithRetry(
            [&, this]() { return reader.seek(recovered_message_id); }, "seekWithMessageId", /*timeout_ms=*/2'000, logger);

        if (res != pulsar::ResultOk)
            throw Exception(
                ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                "Failed to seek to checkpointed message_id {} on topic {}: {}",
                formatMessageId(recovered_message_id),
                getTopic(),
                pulsar::strResult(res));
    });
}

Strings PulsarSource::doFetchData(const Streaming::SequenceRange & sn_range)
{
    auto last_sn_range = lastProcessedSNRange();
    if (!latest_consumed_batch_begin_message_id || last_sn_range.start < 0)
    {
        LOG_INFO(
            logger,
            "Fetching data of sn_range ({}, {}), but no messages were consumed from topic {} yet",
            sn_range.start,
            sn_range.end,
            getTopic());
        return {};
    }

    auto start = sn_range.start;
    auto end = sn_range.end;
    if (end < last_sn_range.start)
    {
        /// All data are gone, show something to the users so that they won't be confused why messages are empty.
        return {fmt::format("Messages of sn range ({}, {}) were lost track", start, end)};
    }

    start = std::max(start, last_sn_range.start);
    end = std::min(end, last_sn_range.end);

    auto skip = start - last_sn_range.start;
    auto count = end - start + 1;

    LOG_INFO(
        logger,
        "Fetching data sn_range=({}, {}) actual_range=({},{}) skip={} count={}",
        sn_range.start,
        sn_range.end,
        start,
        end,
        skip,
        count);

    if (count < 1)
        return {};

    Strings results;
    results.reserve(count);

    auto local_reader = storage->createReader(query_context, getTopic());
    SCOPE_EXIT_SAFE(local_reader.close());

    auto res = executeWithRetry(
        [&, this]() { return local_reader.seek(*latest_consumed_batch_begin_message_id); },
        "seekWithMessageId",
        /*timeout_ms=*/2'000,
        logger);

    if (res != pulsar::ResultOk)
        throw Exception(
            ErrorCodes::CANNOT_RECEIVE_MESSAGE,
            "Failed to seek to message_id {} on topic {} for fetching data sn_range=({}, {}): {}",
            formatMessageId(*latest_consumed_batch_begin_message_id),
            getTopic(),
            sn_range.start,
            sn_range.end,
            pulsar::strResult(res));

    pulsar::Message msg;
    while (count > 0)
    {
        res = local_reader.readNext(msg, static_cast<int>(generate_timeout_ms));

        if (res == pulsar::ResultTimeout || res == pulsar::ResultAlreadyClosed) /// That means no more messages to read
            break;

        if (res != pulsar::ResultOk)
            throw Exception(
                ErrorCodes::CANNOT_RECEIVE_MESSAGE, "Failed to fetch message from topic {}: {}", getTopic(), pulsar::strResult(res));

        if (skip > 0)
        {
            --skip;
            continue;
        }

        results.push_back(msg.getDataAsString());
        --count;
    }

    return results;
}
}

}

#endif
