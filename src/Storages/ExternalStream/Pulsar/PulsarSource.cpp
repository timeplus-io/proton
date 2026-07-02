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
    if (timeout_setting <= 0)
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "record_consume_timeout_ms must be greater than 0");

    return timeout_setting;
}

}

PulsarSource::PulsarSource(
    const Block & header_,
    const StorageSnapshotPtr & storage_snapshot_,
    std::map<size_t, std::pair<DataTypePtr, std::function<Field(const pulsar::Message &)>>> virtual_header_,
    bool is_streaming_,
    const String & data_format_,
    const FormatSettings & format_settings_,
    pulsar::Reader && reader_,
    const std::shared_ptr<Pulsar> & storage_,
    ExternalStreamCounterPtr counter,
    LoggerPtr logger_,
    const ContextPtr & context_)
    : Streaming::ISource(header_, true, logger_, ProcessorID::PulsarSourceID)
    , ExternalStreamSource(header_, storage_snapshot_, context_->getSettingsRef().max_block_size.value, context_)
    , virtual_header(virtual_header_)
    , generate_batch_count(std::min<UInt64>(
          std::max<UInt64>(context_->getSettingsRef().record_consume_batch_count, 1), context_->getSettingsRef().max_block_size))
    , generate_timeout_ms(getGenerateTimeoutMs(context_))
    , storage(storage_)
    , reader(std::move(reader_))
    , ignore_format_errors(format_settings_.ignore_parsing_errors)
    , external_stream_counter(counter)
    , data_format(data_format_)
    , format_settings(format_settings_)
{
    setStreaming(is_streaming_);
    if (!is_streaming_)
    {
        auto res = executeWithRetry(
            [&, this]() { return reader.getLastMessageId(end_message_id); }, "getLastMessageId", /*timeout_ms=*/2'000, logger);

        if (res != pulsar::ResultOk)
            throw Exception(
                ErrorCodes::CANNOT_CONNECT_SERVER, "Failed to get last message id of topic {}: {}", getTopic(), pulsar::strResult(res));

        LOG_DEBUG(
            logger, "Last message ID for non-streaming read: topic='{}' message_id={}", getTopic(), formatMessageId(end_message_id));

        /// Check if topic is empty
        if (end_message_id.entryId() == -1)
            is_finished = true;
    }

    getPhysicalHeader();

    std::tie(format_executor, format_batch_executor) = getInputFormatExecutor(data_format, format_settings);

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

    /// Phase 1: Read messages from Pulsar
    std::vector<pulsar::Message> messages;
    messages.reserve(generate_batch_count);

    Int64 consumed_messages = 0;
    pulsar::MessageId last_consumed_message_id;

    auto timeout_ms = generate_timeout_ms;
    Stopwatch stopwatch;
    while (timeout_ms > 0 && messages.size() < generate_batch_count && !isCancelled() && !is_finished)
    {
        pulsar::Message msg;
        auto res = reader.readNext(msg, static_cast<int>(timeout_ms));
        if (res == pulsar::ResultTimeout || res == pulsar::ResultAlreadyClosed)
        {
            /// For non-streaming queries, if there is no message available, no needs to wait for more messages.
            if (!is_streaming && consumed_messages == 0)
            {
                LOG_WARNING(logger, "Finish non-streaming query read for no messages received from '{}' in {} ms (record_consume_timeout_ms)", getTopic(), timeout_ms);
                is_finished = true;
            }
            break;
        }

        if (res != pulsar::ResultOk)
            throw Exception(
                ErrorCodes::CANNOT_RECEIVE_MESSAGE, "Failed to receive message from topic {}: {}", getTopic(), pulsar::strResult(res));

        if (!is_streaming && msg.getMessageId() >= end_message_id)
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
        messages.push_back(std::move(msg));
    }

    if (messages.empty())
        return header_chunk.clone();

    /// Phase 2: Parse messages
    MutableColumns batch;
    size_t rows = 0;
    bool batch_parse_succeed = false;

    /// Try batch parsing first if available
    if (format_batch_executor)
    {
        std::vector<StringRef> message_refs;
        message_refs.reserve(messages.size());
        for (const auto & msg : messages)
            message_refs.emplace_back(static_cast<const char *>(msg.getData()), msg.getLength());

        try
        {
            rows = parseMessagesInBatch(message_refs);
            batch = format_batch_executor->getResultColumns();
            batch_parse_succeed = true;
            last_consumed_message_id = messages.back().getMessageId();

            if (!virtual_header.empty())
            {
                /// Check if rows match message count (each message = one row assumption)
                if (rows == messages.size())
                {
                    auto virtual_cols = generateVirtualColumns(messages);

                    /// Merge physical and virtual columns
                    MutableColumns result;
                    result.reserve(header_chunk.getNumColumns());
                    for (size_t pos = 0, physical_idx = 0, virtual_idx = 0; pos < header_chunk.getNumColumns(); ++pos)
                    {
                        if (!virtual_header.contains(pos))
                            result.push_back(std::move(batch[physical_idx++]));
                        else
                            result.push_back(std::move(virtual_cols[virtual_idx++]));
                    }
                    batch = std::move(result);
                }
                else
                {
                    /// Rows don't match message count, fall back to parse one by one
                    LOG_DEBUG(
                        logger,
                        "Batch parsing rows ({}) != message count ({}). Consider disable batch parsing by setting "
                        "'parse_messages_in_batch=0'",
                        rows,
                        messages.size());

                    rows = 0;
                    batch.clear();
                    batch_parse_succeed = false;
                }
            }
        }
        catch (...)
        {
            /// Fall back to per-message parse
            rows = 0;
            batch.clear();
            batch_parse_succeed = false;
        }
    }

    if (!batch_parse_succeed)
    {
        /// Reset format executor
        if (format_batch_executor)
            format_batch_executor->getResultColumns();

        /// Parse individually
        for (const auto & msg : messages)
        {
            ReadBufferFromMemory buf(static_cast<const char *>(msg.getData()), msg.getLength());
            size_t new_rows = 0;
            try
            {
                new_rows = format_executor->execute(buf);
                last_consumed_message_id = msg.getMessageId();
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
                    break;
                }
            }

            /// Generate virtual columns
            if (!virtual_header.empty())
            {
                if (!batch.empty())
                {
                    for (size_t pos = 0; pos < batch.size(); ++pos)
                    {
                        if (virtual_header.contains(pos))
                            batch[pos]->insertMany(virtual_header[pos].second(msg), new_rows);
                    }
                }
                else
                {
                    batch.resize(header_chunk.getNumColumns());
                    for (size_t pos = 0; pos < header_chunk.getNumColumns(); ++pos)
                    {
                        if (virtual_header.contains(pos))
                        {
                            auto vheader = virtual_header[pos];
                            batch[pos] = vheader.first->createColumn();
                            batch[pos]->insertMany(vheader.second(msg), new_rows);
                        }
                    }
                }
            }
            rows += new_rows;
        }

        auto new_data = format_executor->getResultColumns();
        if (rows > 0)
        {
            if (virtual_header.empty())
            {
                batch = std::move(new_data);
            }
            else
            {
                for (size_t pos = 0, i = 0; pos < header_chunk.getNumColumns(); ++pos)
                {
                    if (!virtual_header.contains(pos))
                        batch[pos] = std::move(new_data[i++]);
                }
            }
        }
    }

    if (consumed_messages > 0)
    {
        auto last_processed_sn = lastProcessedSN();
        setLastProcessedSNRange({.start = last_processed_sn + 1, .end = last_processed_sn + consumed_messages});
        latest_consumed_message_id = last_consumed_message_id;
    }

    if (rows != 0u)
    {
        external_stream_counter->addReadRows(rows);
        return {std::move(batch), rows};
    }
    else
        return header_chunk.clone();
}

size_t PulsarSource::parseMessagesInBatch(const std::vector<StringRef> & message_refs)
{
    /// Calculate total bytes and reserve buffer
    size_t total_bytes = 0;
    for (const auto & msg : message_refs)
        total_bytes += msg.size;

    batch_buffer.clear();
    batch_buffer.reserve(total_bytes + (data_format == "ProtobufSingle" ? message_refs.size() * 2 : 0) + 128);

    SCOPE_EXIT({
        /// Prevent buffer from keeping growing indefinitely
        if (batch_buffer.capacity() > 64 * 1024 * 1024)
        {
            LOG_INFO(
                logger,
                "Messages buffer is cleared to release memory: topic={} capacity={}",
                getTopic(),
                batch_buffer.capacity());

            std::string empty;
            batch_buffer.swap(empty);
        }
    });

    /// Write messages to buffer
    WriteBufferFromString wb(batch_buffer);
    for (const auto & message : message_refs)
    {
        /// Protobuf format should have varint message length as delimiter
        if (data_format == "ProtobufSingle")
            writeStringBinary(message, wb);
        else
            writeString(message, wb);
    }
    wb.finalize();

    /// Execute parsing
    ReadBufferFromMemory rb(batch_buffer);
    return format_batch_executor->execute(rb);
}

MutableColumns PulsarSource::generateVirtualColumns(const std::vector<pulsar::Message> & messages)
{
    MutableColumns virtual_cols;
    virtual_cols.reserve(virtual_header.size());

    for (const auto & [pos, type_and_func] : virtual_header)
    {
        auto column = type_and_func.first->createColumn();
        column->reserve(messages.size());
        for (const auto & msg : messages)
            column->insert(type_and_func.second(msg));
        virtual_cols.push_back(std::move(column));
    }

    return virtual_cols;
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

void PulsarSource::doCheckpoint(CheckpointContextPtr ckpt_ctx_)
{
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
