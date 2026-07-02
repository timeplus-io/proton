#include <Storages/ExternalStream/NATSJetstream/NATSJetstreamSink.h>

#if USE_NATSIO

#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Interpreters/Context.h>
#include <Storages/ExternalStream/NATSJetstream/NATSJetstream.h>
#include <base/sleep.h>
#include <Common/ProtonCommon.h>

namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_CONNECT_SERVER;
extern const int TIMEOUT_EXCEEDED;
}

namespace ExternalStream
{

namespace
{

/// Extract NATS message headers from Map(String, String) column
std::unordered_map<String, String> getHeaders(const IColumn & column, size_t row)
{
    const auto & column_map = assert_cast<const ColumnMap &>(column);
    const auto & column_array = column_map.getNestedColumn();
    const auto & offsets = column_array.getOffsets();
    size_t offset = (row > 0) ? offsets[row - 1] : 0;
    size_t next_offset = offsets[row];
    size_t row_count = next_offset - offset;
    const auto & nested_columns = column_map.getNestedData();
    const auto & keys_column = nested_columns.getColumn(0);
    const auto & values_column = nested_columns.getColumn(1);

    std::unordered_map<String, String> headers;
    headers.reserve(row_count);
    for (size_t i = offset; i < next_offset; ++i)
        headers.emplace(keys_column.getDataAt(i), values_column.getDataAt(i));
    return headers;
}

/// Get message key (dynamic NATS subject) from column
String getKey(const IColumn & column, size_t row)
{
    if (column.isNullable())
    {
        const auto & col = assert_cast<const ColumnNullable &>(column);
        if (col.isNullAt(row))
            return "";
        return getKey(col.getNestedColumn(), row);
    }

    return String(column.getDataAt(row).data, column.getDataAt(row).size);
}

} /// anonymous namespace

NATSJetstreamSink::NATSJetstreamSink(
    NATSJetstream & storage,
    const Block & header,
    ExternalStreamCounterPtr external_stream_counter_,
    LoggerPtr logger_,
    ContextPtr context)
    : SinkToStorage(header, ProcessorID::NATSJetstreamSinkID)
    , js_context(storage.getJetStreamContext())
    , subject(storage.getSubject())
    , one_message_per_row(storage.produceOneMessagePerRow())
    , checkpoint_timeout_ms(context->getSettingsRef().insert_timeout_ms)
    , external_stream_counter(std::move(external_stream_counter_))
    , logger(std::move(logger_))
{
    const auto & data_format = storage.dataFormat();
    assert(!data_format.empty());

    Block output_header = header;

    /// `_tp_message_key` should not be part of the message payload — used for subject routing
    if (auto pos = output_header.tryGetPositionByName(ProtonConsts::RESERVED_MESSAGE_KEY); pos)
    {
        msg_key_column_pos = pos;
        output_header.erase(*pos);
    }

    /// `_tp_time` should not be part of the message payload
    if (auto pos = output_header.tryGetPositionByName(ProtonConsts::RESERVED_EVENT_TIME); pos)
    {
        ts_column_pos = pos;
        output_header.erase(*pos);
    }

    /// `_tp_message_headers` should not be part of the message payload
    if (auto pos = output_header.tryGetPositionByName(ProtonConsts::RESERVED_MESSAGE_HEADERS); pos)
    {
        headers_column_pos = pos;
        output_header.erase(*pos);
    }

    auto max_rows_per_message = context->getSettingsRef().max_insert_block_size.value;
    if (one_message_per_row)
        max_rows_per_message = 1;

    format_executor = std::make_unique<MessageQueueFormatExecutor>(
        output_header,
        data_format,
        storage.getFormatSettings(context),
        max_rows_per_message,
        0, /// max_message_size: unlimited for NATS
        context);
    format_executor->start();
}

NATSJetstreamSink::~NATSJetstreamSink()
{
    try
    {
        onFinish();
    }
    catch (...)
    {
        tryLogCurrentException(logger, "Error during NATS JetStream sink cleanup");
    }
}

void NATSJetstreamSink::sendMessage(
    const String & message,
    ColumnPtr key_col,
    ColumnPtr headers_col,
    ColumnPtr /*ts_col*/)
{
    const char * publish_subject = subject.c_str();
    String dynamic_subject;

    /// _tp_message_key: use the key value as the NATS subject for routing
    if (key_col)
    {
        dynamic_subject = getKey(*key_col, current_batch_row);
        if (!dynamic_subject.empty())
            publish_subject = dynamic_subject.c_str();
    }

    /// Create NATS message with headers
    natsMsg * nats_msg = nullptr;
    natsStatus status = natsMsg_Create(&nats_msg, publish_subject, nullptr, message.data(), static_cast<int>(message.size()));

    if (status != NATS_OK)
    {
        external_stream_counter->addWrittenFailed(1);
        throw Exception(
            ErrorCodes::CANNOT_CONNECT_SERVER,
            "Failed to create NATS message for subject '{}': {}",
            publish_subject,
            natsStatus_GetText(status));
    }

    /// Set headers from _tp_message_headers column
    if (headers_col)
    {
        auto headers = getHeaders(*headers_col, current_batch_row);
        for (const auto & [k, v] : headers)
            natsMsgHeader_Set(nats_msg, k.c_str(), v.c_str());
    }

    ++current_batch_row;

    /// Synchronous JetStream publish with ack
    jsPubAck * pub_ack = nullptr;
    jsPubOptions pub_opts;
    jsPubOptions_Init(&pub_opts);

    status = js_PublishMsg(&pub_ack, js_context, nats_msg, &pub_opts, nullptr);
    natsMsg_Destroy(nats_msg);

    if (status != NATS_OK)
    {
        ++state.error_count;
        state.last_error_code.store(static_cast<int32_t>(status));
        external_stream_counter->addWrittenFailed(1);
        throw Exception(
            ErrorCodes::CANNOT_CONNECT_SERVER,
            "Failed to publish message to JetStream subject '{}': {}",
            publish_subject,
            natsStatus_GetText(status));
    }

    ++state.outstanding;
    ++state.acked;

    /// Duplicate detection
    if (pub_ack && pub_ack->Duplicate)
    {
        LOG_WARNING(logger, "Duplicate message detected on subject '{}' seq={}", publish_subject, pub_ack->Sequence);
    }

    if (pub_ack)
        jsPubAck_Destroy(pub_ack);

    external_stream_counter->addWrittenBytes(message.size());
    external_stream_counter->addWrittenRows(1);
}

void NATSJetstreamSink::consume(Chunk chunk)
{
    if (!chunk.hasRows())
        return;

    if (unlikely(is_finished.test()))
        throw Exception(
            ErrorCodes::CANNOT_CONNECT_SERVER,
            "NATSJetstreamSink cannot consume data because the sink has stopped");

    auto block = getHeader().cloneWithColumns(chunk.detachColumns());

    current_batch_row = 0;

    /// Extract virtual columns before erasing. Collect positions to erase
    /// in descending order so earlier erases don't shift later positions.
    ColumnPtr key_column{nullptr};
    ColumnPtr ts_column{nullptr};
    ColumnPtr headers_column{nullptr};

    /// Gather positions to erase (sorted descending to avoid index shifting)
    std::vector<size_t> erase_positions;

    if (msg_key_column_pos)
    {
        key_column = block.getByPosition(*msg_key_column_pos).column;
        erase_positions.push_back(*msg_key_column_pos);
    }
    if (ts_column_pos)
    {
        ts_column = block.getByPosition(*ts_column_pos).column;
        erase_positions.push_back(*ts_column_pos);
    }
    if (headers_column_pos)
    {
        headers_column = block.getByPosition(*headers_column_pos).column;
        erase_positions.push_back(*headers_column_pos);
    }

    /// Erase in reverse order so indices remain valid
    std::sort(erase_positions.rbegin(), erase_positions.rend());
    for (auto pos : erase_positions)
        block.erase(pos);

    format_executor->execute(
        block,
        [this, &ts_column, &key_column, &headers_column](const String & message, size_t) {
            sendMessage(message, key_column, headers_column, ts_column);
        },
        /*flush_at_the_end=*/true);
}

void NATSJetstreamSink::onFinish()
{
    if (is_finished.test_and_set())
        return;

    LOG_INFO(logger, "Stopping NATS JetStream sink: subject='{}'", subject);

    format_executor->finish();

    if (outstandingMessages() > 0)
    {
        LOG_ERROR(
            logger,
            "Not all messages were acknowledged: outstanding={} acked={} errors={}",
            state.outstanding.load(),
            state.acked.load(),
            state.error_count.load());
    }
}

void NATSJetstreamSink::doCheckpoint(CheckpointContextPtr context)
{
    /// For synchronous JetStream publish, all messages are acked inline.
    /// But we still verify counts for correctness.
    Stopwatch timer;
    do
    {
        if (state.last_error_code.load() != 0)
            throw Exception(
                ErrorCodes::CANNOT_CONNECT_SERVER,
                "NATS JetStream sink checkpoint failed: error_count={} last_error={}",
                state.error_count.load(),
                natsStatus_GetText(static_cast<natsStatus>(state.last_error_code.load())));

        auto outstanding_msgs = outstandingMessages();
        if (outstanding_msgs == 0)
            break;

        if (timer.elapsedMilliseconds() >= checkpoint_timeout_ms)
            throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "NATS JetStream checkpoint timed out, outstanding={}", outstanding_msgs);

        LOG_INFO(logger, "Waiting for {} outstanding during checkpoint", outstanding_msgs);

        sleepForMilliseconds(10);
    } while (true);

    state.reset();

    IProcessor::doCheckpoint(context);
}

void NATSJetstreamSink::State::reset()
{
    outstanding.store(0);
    acked.store(0);
    error_count.store(0);
    last_error_code.store(0);
}

} /// namespace ExternalStream

} /// namespace DB

#endif
