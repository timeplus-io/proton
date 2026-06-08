#include <Storages/ExternalStream/Pulsar/PulsarSink.h>

#if USE_PULSAR

#include <Formats/FormatFactory.h>
#include <base/sleep.h>
#include <Common/ProtonCommon.h>
#include <Common/logger_useful.h>

#include <pulsar/MessageBuilder.h>
#include <pulsar/Result.h>

#include <thread>

namespace DB
{

namespace ErrorCodes
{
extern const int INVALID_DATA;
extern const int TIMEOUT_EXCEEDED;
}

namespace
{
void setMessageProperties(pulsar::MessageBuilder & b, const IColumn & headers_col, size_t row)
{
    const auto & column_map = assert_cast<const ColumnMap &>(headers_col);
    const auto & column_array = column_map.getNestedColumn();
    const auto & offsets = column_array.getOffsets();
    size_t offset = offsets[row - 1];
    size_t next_offset = offsets[row];
    const auto & nested_columns = column_map.getNestedData();
    const auto & keys_column = nested_columns.getColumn(0);
    const auto & values_column = nested_columns.getColumn(1);

    for (size_t i = offset; i < next_offset; ++i)
        b.setProperty(keys_column.getDataAt(i).toString(), values_column.getDataAt(i).toString());
}
}


namespace ExternalStream
{

PulsarSink::PulsarSink(
    const Block & header,
    pulsar::Producer && producer_,
    const String & data_format,
    const FormatSettings & format_settings,
    UInt64 max_rows_per_message,
    UInt64 max_message_size,
    ExternalStreamCounterPtr external_stream_counter_,
    LoggerPtr logger_,
    const ContextPtr & context)
    : SinkToStorage(header, ProcessorID::ExternalTableDataSinkID)
    , producer(std::move(producer_))
    , send_async_callback([this](pulsar::Result res, const pulsar::MessageId & message_id) { onSent(res, message_id); })
    , ack_timeout_ms(context->getSettingsRef().insert_timeout_ms)
    , external_stream_counter(std::move(external_stream_counter_))
    , logger(std::move(logger_))
{
    auto output_header = header.cloneEmpty();
    /// `_tp_message_key` should not be part of the message payload.
    if (auto pos = output_header.tryGetPositionByName(ProtonConsts::RESERVED_MESSAGE_KEY); pos)
    {
        msg_key_pos = pos;
        output_header.erase(*pos);
    }
    /// `_tp_message_headers` should not be part of the message payload.
    if (auto pos = output_header.tryGetPositionByName(ProtonConsts::RESERVED_MESSAGE_HEADERS); pos)
    {
        msg_headers_pos = pos;
        output_header.erase(*pos);
    }

    format_executor = std::make_unique<MessageQueueFormatExecutor>(
        std::move(output_header), data_format, format_settings, max_rows_per_message, max_message_size, context);
    format_executor->start();
}

PulsarSink::~PulsarSink()
{
    onFinish();
}

void PulsarSink::onFinish()
{
    LOG_INFO(logger, "Finishing");

    if (stopped.test_and_set())
        return;

    if (format_executor)
        format_executor->finish();
    waitForAcks();
    producer.flush();
    producer.close();
}

void PulsarSink::consume(Chunk chunk)
{
    if (!chunk.hasRows())
        return;

    auto block = getHeader().cloneWithColumns(chunk.detachColumns());
    ColumnPtr key_column;
    if (msg_key_pos)
    {
        key_column = block.getByPosition(*msg_key_pos).column;
        block.erase(*msg_key_pos);
    }

    ColumnPtr headers_column;
    if (msg_headers_pos)
    {
        headers_column = block.getByPosition(*msg_headers_pos).column;
        block.erase(*msg_headers_pos);
    }

    size_t row{0};
    format_executor->execute(
        block,
        [this, &row, &key_column, &headers_column](const String & message, size_t) {
            onMessageReady(message, key_column, headers_column, row++);
        },
        /*flush_at_the_end=*/true);
}

void PulsarSink::onMessageReady(const String & message, ColumnPtr key_col, ColumnPtr headers_col, size_t row)
{
    pulsar::MessageBuilder b;
    if (key_col)
        b.setPartitionKey(key_col->getDataAt(row).toString());

    if (headers_col)
        setMessageProperties(b, *headers_col, row);

    b.setContent(message);
    producer.sendAsync(b.build(), send_async_callback);
    ++state.outstandings;
    external_stream_counter->addToMessagesBySize(1);
}

void PulsarSink::onSent(pulsar::Result res, const pulsar::MessageId &)
{
    if (stopped.test())
        return;

    ++state.acked;

    if (res != pulsar::ResultOk)
    {
        ++state.error_count;
        state.last_error_code = res;
    }
}

void PulsarSink::waitForAcks() const
{
    bool waiting_logged = false;

    Stopwatch timer;

    do
    {
        if (stopped.test())
            break;

        pulsar::Result last_result = static_cast<pulsar::Result>(state.last_error_code.load());
        if (last_result != pulsar::ResultOk)
            throw Exception(
                ErrorCodes::INVALID_DATA,
                "Failed to send messages, error_count={} last_error={}",
                state.error_count,
                pulsar::strResult(last_result));

        auto acked = state.acked.load();
        if (state.outstandings == acked)
            break;

        if (!waiting_logged)
        {
            LOG_INFO(logger, "Waiting for {} acks", state.outstandings - acked);
            waiting_logged = true;
        }

        if (timer.elapsedMilliseconds() >= ack_timeout_ms)
            throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Timed out waiting for acks, missing_acks={}", state.outstandings - acked);

        sleepForMilliseconds(100);
    } while (true);

    if (waiting_logged)
        LOG_INFO(logger, "All message acked");
}

void PulsarSink::checkpoint(CheckpointContextPtr context)
{
    /// Flush any pending messages in Pulsar's internal batch buffer before waiting for acks.
    /// Without this, messages may be stuck in the batch buffer waiting for the batch timer,
    /// causing waitForAcks() to timeout even though messages haven't been sent yet.
    producer.flush();
    waitForAcks();

    state.reset();
    IProcessor::checkpoint(context);
}

void PulsarSink::State::reset()
{
    outstandings.store(0);
    acked.store(0);
    error_count.store(0);
    last_error_code.store(0);
}

}

}
#endif
