#include <Storages/ExternalStream/NATSJetstream/NATSJetstreamSource.h>

#if USE_NATSIO

#include <Checkpoint/CheckpointContext.h>
#include <Checkpoint/CheckpointCoordinator.h>
#include <Columns/ColumnMap.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Storages/ExternalStream/ExternalStreamCounter.h>
#include <Storages/ExternalStream/NATSJetstream/NATSJetstream.h>
#include <Common/ProtonCommon.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_RECEIVE_MESSAGE;
extern const int RECOVER_CHECKPOINT_FAILED;
}

namespace ExternalStream
{

/// ======== StallDetector ========

NATSJetstreamSource::StallDetector::StallDetector(UInt64 timeout_ms_, LoggerPtr logger_)
    : timeout_ms(timeout_ms_), logger(std::move(logger_))
{
}

void NATSJetstreamSource::StallDetector::checkAndHandleStall(NATSJetstreamSource & source)
{
    if (timeout_ms == 0)
        return;

    if (timer.elapsedMilliseconds() < timeout_ms)
        return;

    auto last_processed_sn = source.lastProcessedSN();

    /// If we've made progress since last check, reset timers
    if (last_processed_sn != recorded_last_processed_sn)
    {
        recorded_last_processed_sn = last_processed_sn;
        timer.restart();
        caught_up_timer.restart();
        return;
    }

    /// If we haven't consumed any messages at all and there may be no messages, use a longer timeout
    if (last_processed_sn < 0 && caught_up_timer.elapsedMilliseconds() < 10 * timeout_ms)
        return;

    /// Recreate subscription
    LOG_WARNING(
        logger,
        "Consumer seemed stalled at sn={} for more than {} ms, recreating subscription",
        recorded_last_processed_sn,
        timeout_ms);

    try
    {
        if (source.subscription)
        {
            natsSubscription_Drain(source.subscription);
            natsSubscription_Destroy(source.subscription);
        }
        source.subscription = source.storage->createPullSubscription(source.current_consumer_name, source.query_context);
    }
    catch (...)
    {
        tryLogCurrentException(logger, "Failed to recreate subscription during stall recovery");
    }

    reset();
}

void NATSJetstreamSource::StallDetector::onProgress()
{
    timer.restart();
    caught_up_timer.restart();
}

void NATSJetstreamSource::StallDetector::reset()
{
    recorded_last_processed_sn = -1;
    timer.restart();
    caught_up_timer.restart();
}

/// ======== NATSJetstreamSource ========

NATSJetstreamSource::NATSJetstreamSource(
    const Block & header_,
    const StorageSnapshotPtr & storage_snapshot_,
    bool is_streaming_,
    const String & data_format,
    const FormatSettings & format_settings,
    natsSubscription * subscription_,
    std::shared_ptr<NATSJetstream> storage_,
    const String & consumer_name_,
    size_t max_block_size_,
    UInt64 stall_timeout_ms,
    ExternalStreamCounterPtr counter,
    LoggerPtr logger_,
    const ContextPtr & context_)
    : Streaming::ISource(header_, true, logger_, ProcessorID::NATSJetstreamSourceID)
    , ExternalStreamSource(header_, storage_snapshot_, max_block_size_, context_)
    , virtual_col_value_functions(header.columns(), nullptr)
    , virtual_col_types(header.columns(), nullptr)
    , storage(std::move(storage_))
    , subscription(subscription_)
    , current_subject(storage->getSubject())
    , current_consumer_name(consumer_name_)
    , ignore_format_errors(format_settings.ignore_parsing_errors)
    , stall_detector(stall_timeout_ms, logger_)
    , external_stream_counter(std::move(counter))
{
    assert(external_stream_counter);

    setStreaming(is_streaming_);

    if (auto consume_timeout = context_->getSettingsRef().record_consume_timeout_ms; consume_timeout != 0)
        record_consume_timeout_ms = static_cast<Int32>(consume_timeout.value);

    getPhysicalHeader();
    format_executor = getInputFormatExecutor(data_format, format_settings).first;

    header_chunk = Chunk(header.getColumns(), 0);

    setDescription(fmt::format("subject={} stream={}", current_subject, storage->getStreamName()));
}

NATSJetstreamSource::~NATSJetstreamSource()
{
    if (!isCancelled())
        onCancel();
}

/// ======== getPhysicalHeader ========
void NATSJetstreamSource::getPhysicalHeader()
{
    auto non_virtual_header = storage_snapshot->metadata->getSampleBlockNonMaterialized();

    for (size_t pos = 0; const auto & column : header)
    {
        /// _tp_message_key: extracts NATS subject as a string
        if (column.name == ProtonConsts::RESERVED_MESSAGE_KEY)
        {
            bool inside_nullable = false;
            auto type_id = column.type->getTypeId();
            if (type_id == TypeIndex::Nullable)
            {
                type_id = (assert_cast<const DataTypeNullable &>(*column.type)).getNestedType()->getTypeId();
                inside_nullable = true;
            }

            /// NATS subjects are always strings, so we only support String/FixedString for _tp_message_key
            virtual_col_value_functions[pos] = [inside_nullable](natsMsg * msg) -> Field {
                const char * subj = natsMsg_GetSubject(msg);
                if (!subj || strlen(subj) == 0)
                {
                    if (inside_nullable)
                        return Null{};
                    return String{};
                }
                return String(subj);
            };
            virtual_col_types[pos] = column.type;
        }
        /// _tp_message_headers: extracts NATS message headers as Map(String, String)
        else if (column.name == ProtonConsts::RESERVED_MESSAGE_HEADERS)
        {
            virtual_col_value_functions[pos] = [](natsMsg * msg) -> Field {
                Map result;

                const char ** keys = nullptr;
                int num_keys = 0;
                natsStatus status = natsMsgHeader_Keys(msg, &keys, &num_keys);

                if (status != NATS_OK || !keys || num_keys == 0)
                    return result;

                result.reserve(num_keys);
                for (int i = 0; i < num_keys; ++i)
                {
                    const char * value = nullptr;
                    if (natsMsgHeader_Get(msg, keys[i], &value) == NATS_OK && value)
                        result.push_back(Tuple{String(keys[i]), String(value)});
                    else
                        result.push_back(Tuple{String(keys[i]), String("null")});
                }

                /// The keys array must be freed (natsMsgHeader_Keys allocates a plain C array)
                free(keys);
                return result;
            };
            virtual_col_types[pos] = column.type;
        }
        /// _tp_time: NATS message timestamp
        else if (column.name == ProtonConsts::RESERVED_EVENT_TIME)
        {
            virtual_col_value_functions[pos] = [](natsMsg * msg) -> Field {
                jsMsgMetaData * meta = nullptr;
                if (natsMsg_GetMetaData(&meta, msg) == NATS_OK && meta)
                {
                    int64_t ts_ns = meta->Timestamp;
                    jsMsgMetaData_Destroy(meta);
                    if (ts_ns > 0)
                        return Decimal64(ts_ns / 1'000'000); /// ns -> ms
                }
                return Decimal64();
            };
            virtual_col_types[pos] = column.type;
        }
        /// Physical columns that are also in the non-virtual header
        else if (std::ranges::any_of(
                     non_virtual_header, [&column](auto & non_virtual_column) { return non_virtual_column.name == column.name; }))
        {
            physical_header.insert(column);
        }
        /// _tp_append_time
        else if (column.name == ProtonConsts::RESERVED_APPEND_TIME)
        {
            virtual_col_value_functions[pos] = [](natsMsg * msg) -> Field {
                jsMsgMetaData * meta = nullptr;
                if (natsMsg_GetMetaData(&meta, msg) == NATS_OK && meta)
                {
                    int64_t ts_ns = meta->Timestamp;
                    jsMsgMetaData_Destroy(meta);
                    if (ts_ns > 0)
                        return Decimal64(ts_ns / 1'000'000); /// ns -> ms
                }
                return Decimal64();
            };
            virtual_col_types[pos] = column.type;
        }
        /// _tp_process_time
        else if (column.name == ProtonConsts::RESERVED_PROCESS_TIME)
        {
            virtual_col_value_functions[pos] = [](natsMsg *) -> Field { return Decimal64(UTCMilliseconds::now()); };
            virtual_col_types[pos] = column.type;
        }
        /// _tp_shard: always 0 for NATS JetStream
        else if (column.name == ProtonConsts::RESERVED_SHARD)
        {
            virtual_col_value_functions[pos] = [](natsMsg *) -> Field { return Int64(0); };
            virtual_col_types[pos] = column.type;
        }
        /// _tp_sn: stream sequence number
        else if (column.name == ProtonConsts::RESERVED_EVENT_SEQUENCE_ID)
        {
            virtual_col_value_functions[pos] = [](natsMsg * msg) -> Field {
                jsMsgMetaData * meta = nullptr;
                if (natsMsg_GetMetaData(&meta, msg) == NATS_OK && meta)
                {
                    auto seq = static_cast<Int64>(meta->Sequence.Stream);
                    jsMsgMetaData_Destroy(meta);
                    return seq;
                }
                return Int64(0);
            };
            virtual_col_types[pos] = column.type;
        }
        /// _nats_subject
        else if (column.name == "_nats_subject")
        {
            virtual_col_value_functions[pos] = [](natsMsg * msg) -> Field { return String(natsMsg_GetSubject(msg)); };
            virtual_col_types[pos] = column.type;
        }
        /// _nats_timestamp
        else if (column.name == "_nats_timestamp")
        {
            virtual_col_value_functions[pos] = [](natsMsg * msg) -> Field {
                jsMsgMetaData * meta = nullptr;
                if (natsMsg_GetMetaData(&meta, msg) == NATS_OK && meta)
                {
                    int64_t ts_ns = meta->Timestamp;
                    jsMsgMetaData_Destroy(meta);
                    return Decimal64(ts_ns / 1'000'000); /// ns -> ms
                }
                return Decimal64();
            };
            virtual_col_types[pos] = column.type;
        }
        else
        {
            /// Unknown column — physical
            physical_header.insert(column);
        }

        ++pos;
    }

    request_virtual_columns = std::ranges::any_of(virtual_col_types, [](auto type_ptr) { return type_ptr != nullptr; });

    /// Clients like to read virtual columns only, add the first physical column so we know row count
    if (physical_header.columns() == 0)
    {
        const auto & physical_columns = storage_snapshot->getColumns(GetColumnsOptions::Ordinary);
        const auto & physical_column = physical_columns.front();
        physical_header.insert({physical_column.type->createColumn(), physical_column.type, physical_column.name});
    }
}

/// ======== parseFormat ========
void NATSJetstreamSource::parseFormat(natsMsg * msg)
{
    assert(format_executor);

    const char * data = natsMsg_GetData(msg);
    int data_len = natsMsg_GetDataLength(msg);

    if (!data || data_len <= 0)
        return;

    ReadBufferFromMemory buffer(data, static_cast<size_t>(data_len));
    auto new_rows = format_executor->execute(buffer);

    external_stream_counter->addReadBytes(data_len);
    external_stream_counter->addReadRows(new_rows);

    if (new_rows == 0u)
        return;

    auto result_block = physical_header.cloneWithColumns(format_executor->getResultColumns());
    MutableColumns new_data(result_block.mutateColumns());

    if (!request_virtual_columns)
    {
        if (!current_batch.empty())
        {
            /// Merge all data in the current batch into the same chunk
            for (size_t pos = 0; pos < current_batch.size(); ++pos)
                current_batch[pos]->insertRangeFrom(*new_data[pos], 0, new_rows);
        }
        else
        {
            current_batch = std::move(new_data);
        }
    }
    else
    {
        /// Slow path: interleave virtual columns
        if (!current_batch.empty())
        {
            assert(current_batch.size() == virtual_col_value_functions.size());

            for (size_t i = 0, j = 0, n = virtual_col_value_functions.size(); i < n; ++i)
            {
                if (!virtual_col_value_functions[i])
                {
                    /// non-virtual column: physical or calculated
                    current_batch[i]->insertRangeFrom(*new_data[j], 0, new_rows);
                    ++j;
                }
                else
                {
                    current_batch[i]->insertMany(virtual_col_value_functions[i](msg), new_rows);
                }
            }
        }
        else
        {
            for (size_t i = 0, j = 0, n = virtual_col_value_functions.size(); i < n; ++i)
            {
                if (!virtual_col_value_functions[i])
                {
                    current_batch.push_back(std::move(new_data[j]));
                    ++j;
                }
                else
                {
                    auto column = virtual_col_types[i]->createColumn();
                    column->insertMany(virtual_col_value_functions[i](msg), new_rows);
                    current_batch.push_back(std::move(column));
                }
            }
        }
    }
}

/// ======== generate — batch pull loop ========
Chunk NATSJetstreamSource::generate()
{
    if (isCancelled())
        return {};

    if (unlikely(reached_the_end))
        return {};

    if (consume_exception)
        consume_exception->rethrow();

    /// Clear batch state for this round
    current_batch.clear();
    current_batch.reserve(header.columns());

    Int64 consumed_messages = 0;
    auto batch_size = static_cast<int>(storage->getBatchSize());

    Stopwatch stopwatch;
    auto timeout_ms = record_consume_timeout_ms;

    while (timeout_ms > 0 && !isCancelled() && !reached_the_end)
    {
        natsMsgList msg_list;
        memset(&msg_list, 0, sizeof(natsMsgList));

        jsFetchRequest fetch_req;
        jsFetchRequest_Init(&fetch_req);
        fetch_req.Batch = batch_size;
        fetch_req.Expires = static_cast<int64_t>(timeout_ms) * 1'000'000; /// ms -> ns
        fetch_req.NoWait = !is_streaming;

        natsStatus status = natsSubscription_FetchRequest(&msg_list, subscription, &fetch_req);

        /// Handle timeout / no-message scenarios
        if (status == NATS_TIMEOUT || status == NATS_NOT_FOUND)
        {
            if (!is_streaming && consumed_messages == 0)
            {
                reached_the_end = true;
            }
            break;
        }

        if (status != NATS_OK)
        {
            natsMsgList_Destroy(&msg_list); /// prevent resource leak before throw
            throw Exception(
                ErrorCodes::CANNOT_RECEIVE_MESSAGE,
                "Failed to fetch messages from NATS JetStream: subject='{}' stream='{}': {}",
                current_subject,
                storage->getStreamName(),
                natsStatus_GetText(status));
        }

        if (msg_list.Count == 0)
        {
            natsMsgList_Destroy(&msg_list);
            if (!is_streaming)
                reached_the_end = true;
            break;
        }

        /// Process each message
        for (int i = 0; i < msg_list.Count && !isCancelled(); ++i)
        {
            natsMsg * msg = msg_list.Msgs[i];
            if (!msg)
                continue;

            ++consumed_messages;

            /// SN-based skip for auto-recovery
            if (messages_to_skip > 0)
            {
                --messages_to_skip;
                /// ACK even skipped messages to prevent redelivery
                if (storage->getAckPolicy() != "none")
                    natsMsg_Ack(msg, nullptr);
                continue;
            }

            try
            {
                parseFormat(msg);
            }
            catch (Exception & e)
            {
                external_stream_counter->addReadFailed(1);

                if (ignore_format_errors)
                {
                    LOG_ERROR(
                        logger,
                        "Failed to parse message subject='{}': {}",
                        natsMsg_GetSubject(msg),
                        e.message());
                }
                else
                {
                    e.addMessage("Failed to parse message subject='{}'", natsMsg_GetSubject(msg));
                    consume_exception.emplace(std::move(e));
                }

                /// ACK bad messages so we don't re-read them forever
                if (storage->getAckPolicy() != "none")
                    natsMsg_Ack(msg, nullptr);
                continue;
            }

            /// ACK after successful parse for at-least-once delivery
            const auto & ack_policy = storage->getAckPolicy();
            if (ack_policy == "explicit")
                natsMsg_Ack(msg, nullptr);
            else if (ack_policy == "all" && i == msg_list.Count - 1)
                natsMsg_Ack(msg, nullptr);
        }

        natsMsgList_Destroy(&msg_list);

        timeout_ms = record_consume_timeout_ms - static_cast<Int32>(stopwatch.elapsedMilliseconds());
    }

    /// Track SN range for this batch
    if (!current_batch.empty())
    {
        auto rows = current_batch[0]->size();
        if (rows > 0 && consumed_messages > 0)
        {
            auto last_sn = lastProcessedSN();
            setLastProcessedSNRange({
                .start = last_sn + 1,
                .end = last_sn + consumed_messages,
            });

            stall_detector.onProgress();

            return {std::move(current_batch), rows};
        }
    }

    /// No data — stall check
    if (consumed_messages == 0)
        stall_detector.checkAndHandleStall(*this);

    /// Heart beat
    return header_chunk.clone();
}

std::pair<Int64, Int64> NATSJetstreamSource::sequenceRange() const
{
    /// NATS JetStream doesn't expose watermark offsets.
    /// Return best-effort range based on what we've processed.
    auto last_sn = lastProcessedSN();
    if (last_sn >= 0)
        return {0, last_sn};
    return {-1, -1};
}

void NATSJetstreamSource::onCancel() noexcept
{
    LOG_INFO(logger, "Cancelling NATS JetStream source: subject='{}'", current_subject);

    reached_the_end = true;

    try
    {
        if (subscription)
        {
            natsSubscription_Drain(subscription);
            natsSubscription_Destroy(subscription);
            subscription = nullptr;
        }
    }
    catch (...)
    {
        tryLogCurrentException(logger, "Failed to close NATS JetStream subscription");
    }
}

void NATSJetstreamSource::doCheckpoint(CheckpointContextPtr ckpt_ctx_)
{
    ckpt_ctx_->coordinator->checkpoint(getVersion(), getLogicID(), ckpt_ctx_, [&](WriteBuffer & wb) {
        writeStringBinary(current_subject, wb);
        writeStringBinary(current_consumer_name, wb);
        writeIntBinary<Int64>(lastProcessedSN(), wb);
    });

    LOG_INFO(logger, "Saved checkpoint subject='{}' consumer='{}' sn={}", current_subject, current_consumer_name, lastProcessedSN());
}

void NATSJetstreamSource::doRecover(CheckpointContextPtr ckpt_ctx_)
{
    ckpt_ctx_->coordinator->recover(getLogicID(), ckpt_ctx_, [&](VersionType, ReadBuffer & rb) {
        if (rb.eof())
        {
            LOG_INFO(logger, "No checkpoint to recover");
            return;
        }

        String recovered_subject;
        readStringBinary(recovered_subject, rb);

        String recovered_consumer_name;
        readStringBinary(recovered_consumer_name, rb);

        if (recovered_subject != current_subject)
            throw Exception(
                ErrorCodes::RECOVER_CHECKPOINT_FAILED,
                "Found mismatched NATS subject. recovered={}, current={}",
                recovered_subject,
                current_subject);

        Int64 recovered_sn;
        readIntBinary<Int64>(recovered_sn, rb);
        setLastCheckpointSN(recovered_sn);

        LOG_INFO(logger, "Recovered checkpoint subject='{}' consumer='{}' last_sn={}", recovered_subject, recovered_consumer_name, recovered_sn);
    });
}

void NATSJetstreamSource::doResetStartSN(Int64 sn)
{
    /// Only called during MV recover before consuming starts
    if (start_consume_flag.test())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected SN reset ({}), consumer had already started", sn);

    if (sn >= 0)
    {
        messages_to_skip = sn - lastProcessedSN();
        if (messages_to_skip < 0)
            messages_to_skip = 0;

        LOG_INFO(logger, "Reset SN to {}, messages_to_skip={}", sn, messages_to_skip);
    }
}

Strings NATSJetstreamSource::doFetchData(const Streaming::SequenceRange & sn_range)
{
    auto last_sn_range = lastProcessedSNRange();

    if (last_sn_range.start < 0)
    {
        LOG_INFO(
            logger,
            "Fetching data of sn_range ({}, {}), but no messages were consumed from subject='{}' yet",
            sn_range.start,
            sn_range.end,
            current_subject);
        return {};
    }

    auto start = sn_range.start;
    auto end = sn_range.end;

    if (end < last_sn_range.start)
    {
        /// All data are gone — show something to users
        return {fmt::format("Messages at sn range ({}, {}) were gone", start, end)};
    }

    start = std::max(start, last_sn_range.start);
    end = std::min(end, last_sn_range.end);
    auto count = end - start + 1;
    LOG_INFO(logger, "Fetching data sn_range=({}, {}) actual_range=({},{}) count={}", sn_range.start, sn_range.end, start, end, count);

    if (count < 1)
        return {};

    Strings results;
    results.reserve(count);

    auto consumer_fetch_name = fmt::format("{}-fetch-{}", current_consumer_name, start);
    auto * fetch_sub = storage->createPullSubscription(consumer_fetch_name, query_context);
    SCOPE_EXIT_SAFE({
        natsSubscription_Drain(fetch_sub);
        natsSubscription_Destroy(fetch_sub);
    });

    auto skip = start - last_sn_range.start;

    while (count > 0)
    {
        natsMsgList msg_list;
        memset(&msg_list, 0, sizeof(natsMsgList));

        jsFetchRequest fetch_req;
        jsFetchRequest_Init(&fetch_req);
        fetch_req.Batch = static_cast<int>(count > 128 ? 128 : count);
        fetch_req.Expires = record_consume_timeout_ms * 1'000'000LL;

        natsStatus status = natsSubscription_FetchRequest(&msg_list, fetch_sub, &fetch_req);

        if (status == NATS_TIMEOUT || status == NATS_NOT_FOUND || msg_list.Count == 0)
        {
            natsMsgList_Destroy(&msg_list);
            break;
        }

        if (status != NATS_OK)
        {
            natsMsgList_Destroy(&msg_list);
            throw Exception(
                ErrorCodes::CANNOT_RECEIVE_MESSAGE,
                "Failed to fetch data from NATS JetStream: {}",
                natsStatus_GetText(status));
        }

        for (int i = 0; i < msg_list.Count; ++i)
        {
            natsMsg * msg = msg_list.Msgs[i];
            if (!msg)
                continue;

            if (skip > 0)
            {
                --skip;
                natsMsg_Ack(msg, nullptr);
                continue;
            }

            const char * data = natsMsg_GetData(msg);
            int data_len = natsMsg_GetDataLength(msg);
            if (data && data_len > 0)
                results.emplace_back(data, data_len);

            natsMsg_Ack(msg, nullptr);
            --count;
        }

        natsMsgList_Destroy(&msg_list);

        /// No progress check
        if (results.empty())
            break;
    }

    return results;
}

} /// namespace ExternalStream

} /// namespace DB

#endif
