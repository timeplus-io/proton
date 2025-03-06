#include <Storages/ExternalStream/Pulsar/PulsarSink.h>

#if USE_PULSAR

#    include <Common/ProtonCommon.h>
#    include <Common/logger_useful.h>

#    include <pulsar/MessageBuilder.h>
#    include <pulsar/Result.h>

#    include <thread>

namespace DB
{

namespace ErrorCodes
{
extern const int INVALID_DATA;
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
    Poco::Logger * logger_,
    const ContextPtr & context)
    : SinkToStorage(header, ProcessorID::ExternalTableDataSinkID)
    , producer(std::move(producer_))
    , send_async_callback([this](pulsar::Result res, const pulsar::MessageId & message_id) { onSent(res, message_id); })
    , write_buffer(
          /*on_next_=*/[this](char * pos, size_t len, size_t total_len) { onWriteBufferNext(pos, len, total_len); },
          /*after_next_=*/[this]() { tryCarryOverPendingData(); },
          /*buffer_size=*/max_message_size)
    , pending_data(max_message_size)
    , external_stream_counter(external_stream_counter_)
    , logger(logger_)
{
    pending_data.resize(0); /// no pending data at the beginning

    Block output_header = header;
    /// `_tp_message_key` should not be part of the message payload.
    if (auto pos = output_header.tryGetPositionByName(ProtonConsts::RESERVED_MESSAGE_KEY); pos)
    {
        generate_message_key = true;
        output_header.erase(*pos);
    }

    writer = FormatFactory::instance().getOutputFormat(
        data_format,
        write_buffer,
        output_header,
        context,
        [this, max_rows_per_message](auto & /*column*/, auto /*row*/) {
            write_buffer.markOffset();
            if (++rows_in_current_message >= max_rows_per_message)
            {
                external_stream_counter->addToMessagesByRow(1);
                write_buffer.next();
            }
        },
        format_settings);

    writer->setAutoFlush();
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

    producer.flush();
    producer.close();
}

void PulsarSink::consume(Chunk chunk)
{
    if (!chunk.hasRows())
        return;

    auto block = getHeader().cloneWithColumns(chunk.detachColumns());
    if (generate_message_key)
    {
        key_pos = 0;
        message_key_column = block.getByName(ProtonConsts::RESERVED_MESSAGE_KEY).column;
        block.erase(ProtonConsts::RESERVED_MESSAGE_KEY);
    }
    writer->write(block);
}

/// When this function is called, there could be two scenarios:
/// 1) len == total_len. That means the whole Chunk (or the whole row, when one_message_per_row is true) can fit into the buffer.
///    In this case, we just need to create a pulsar::Message, and send it out.
///
/// 2) len < total_len. That means the Chunk (or a row) is too big to fit into the buffer. In this case, the data between
///    [pos + len, pos + total_len] will be copied into pending_data so that the bufer can accept more data until it gets
///    complete data. Then it can create a pulsar::Message.
///
/// In this way, we limit the size of one single Pulsar message, so that it can avoid hitting the message size limit (on broker side).
/// However, it's still possible that, one single row is still too big and it exceeds that limit. There is nothing we can do about it for now.
void PulsarSink::onWriteBufferNext(char * pos, size_t len, size_t total_len)
{
    auto pending_size = pending_data.size();

    /// There are complete data to consume.
    if (len > 0)
    {
        pulsar::MessageBuilder b;
        if (message_key_column)
            b.setPartitionKey(message_key_column->getDataAt(key_pos++).toString());

        /// Data at pos (which is in the WriteBuffer) will be overwritten, thus it must be kept somewhere else (in `batch_payload`).
        auto msg_size = pending_size + len;
        nlog::ByteVector payload{msg_size};
        payload.resize(msg_size); /// set the size to the right value
        if (pending_size != 0u)
            memcpy(payload.data(), pending_data.data(), pending_size);
        memcpy(payload.data() + pending_size, pos, len);

        b.setContent({payload.data(), payload.size()});
        producer.sendAsync(b.build(), send_async_callback);
        ++state.outstandings;
        external_stream_counter->addToMessagesBySize(1);

        pending_data.resize(0);
        pending_size = 0;
        rows_in_current_message = 0;
    }

    if (len == total_len)
        /// Nothing left
        return;

    /// There are some remaining incomplete data, copy them to pending_data.
    auto remaining = total_len - len;
    pending_data.resize(pending_size + remaining);
    memcpy(pending_data.data() + pending_size, pos + len, remaining);
}

void PulsarSink::tryCarryOverPendingData()
{
    /// If there are pending data and it can be fit into the buffer, then write the data back to the buffer,
    /// so that we can use the buffer to limit the message size.
    /// If the pending data are too big, that means we get a over-size row.
    if (!pending_data.empty() && pending_data.size() < write_buffer.available())
    {
        write_buffer.write(pending_data.data(), pending_data.size());
        pending_data.resize(0);
    }
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

void PulsarSink::waitForAcks(UInt64 timeout_ms) const
{
    bool waiting_logged = false;

    std::optional<Stopwatch> timer;
    if (timeout_ms > 0)
        timer = Stopwatch();

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

        if (timer.and_then(
                [timeout_ms](const auto & t) { return t.elapsedMilliseconds() > timeout_ms ? std::optional<bool>(true) : std::nullopt; }))
        {
            LOG_INFO(logger, "Waiting for acks timed out, missing acks {}", state.outstandings - acked);
            break;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    } while (true);

    if (waiting_logged)
        LOG_INFO(logger, "All message acked");
}

void PulsarSink::checkpoint(CheckpointContextPtr context)
{
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
