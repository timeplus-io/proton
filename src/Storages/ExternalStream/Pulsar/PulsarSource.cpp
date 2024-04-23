#include "Pulsar.h"
#include "PulsarSource.h"
#include <Processors/Executors/StreamingFormatExecutor.h>
#include <Common/logger_useful.h>

namespace DB{

PulsarSource::PulsarSource(
    Pulsar * pulsar_,
    Block header_,
    const StorageSnapshotPtr & storage_snapshot_,
    ContextPtr query_context_,
    Poco::Logger * log_,
    ExternalStreamCounterPtr external_stream_counter_,
    size_t max_block_size_
    )
    : ISource(header_, true, ProcessorID::FileLogSourceID)
    , pulsar(pulsar_)
    , query_context(query_context_)
    , log(log_)
    , column_type(header_.getByPosition(0).type)
    , virtual_col_value_functions(header.columns(), nullptr)
    , external_stream_counter(external_stream_counter_)
    , max_block_size(max_block_size_)
    , read_buffer("", 0)
    , storage_snapshot(storage_snapshot_)
    , non_virtual_header(storage_snapshot->metadata->getSampleBlockNonMaterialized())
{
    is_streaming = true;
    initConsumer(/*pulsar*/);
    initFormatExecutor(pulsar);
}

PulsarSource::~PulsarSource() = default;

void PulsarSource::initConsumer(/*const Pulsar * pulsar*/)
{
    pulsar::ConsumerConfiguration config;
    config.setSubscriptionInitialPosition(pulsar::InitialPositionEarliest);
    pulsar::Client client("pulsar://localhost:6650");
    pulsar::Result result = client.subscribe("persistent://public/default/my-topic", "consumer-1", config, consumer);
    if (result != pulsar::ResultOk) {
        LOG_ERROR(log, "Failed to initialize consumer");
        return;
    }
}

void PulsarSource::initFormatExecutor(const Pulsar * pulsar_)
{
    const auto & data_format = pulsar_->dataFormat();

    LOG_INFO(log, "IN format 1");

    auto input_format = FormatFactory::instance().getInputFormat(
        data_format,
        read_buffer,
        non_virtual_header,
        query_context,
        max_block_size,
        pulsar_->getFormatSettings(query_context));

    LOG_INFO(log, "IN format 2");

    format_executor = std::make_unique<StreamingFormatExecutor>(
        non_virtual_header,
        std::move(input_format),
        [this](const MutableColumns &, Exception & ex) -> size_t
        {
            format_error = ex.what();
            return 0;
        });

    LOG_INFO(log, "IN format 3");

    auto converting_dag = ActionsDAG::makeConvertingActions(
        non_virtual_header.cloneEmpty().getColumnsWithTypeAndName(),
        physical_header.cloneEmpty().getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Name);

    LOG_INFO(log, "IN format 4");

    convert_non_virtual_to_physical_action = std::make_shared<ExpressionActions>(std::move(converting_dag));
}

Chunk PulsarSource::generate()
{
    LOG_INFO(log, "IN generate");
    if (isCancelled())
        return {};

    if (result_chunks.empty() || iter == result_chunks.end())
    {
        readAndProcess();

        if (isCancelled()) {
            LOG_INFO(log, "cancelled");
            return {};
        }

        if (result_chunks.empty() || iter == result_chunks.end()) {
            LOG_INFO(log, "result_chunks");
            return header_chunk.clone();
        }
    }

    return std::move(*iter++);
//    auto col = column_type->createColumn();
//    col->insertData("kartik", 6);
//    return Chunk(Columns(1, std::move(col)), 1);
}

void PulsarSource::readAndProcess()
{
    pulsar::ConsumerConfiguration config;
    config.setSubscriptionInitialPosition(pulsar::InitialPositionEarliest);
    pulsar::Client client("pulsar://localhost:6650");
    pulsar::Result result = client.subscribe("persistent://public/default/my-topic", "consumer-1", config, consumer);

    if (result != pulsar::ResultOk) {
        LOG_ERROR(log, "Failed to initialize consumer");
        return;
    }
    LOG_INFO(log, "IN readAndProcess");
    result_chunks.clear();
    current_batch.clear();
    current_batch.reserve(header.columns());

    pulsar::Messages messages;
    consumer.batchReceive(messages);
    LOG_INFO(log, "message count: {}", messages.size());

    for (const auto &message: messages) {
        int message_len = message.getLength();
        LOG_INFO(log, "message len: {}", message_len);
        ReadBufferFromMemory buffer(static_cast<const char *>(message.getData()), message_len);
        LOG_INFO(log, "_1");
        auto new_rows = format_executor->execute(buffer);
        external_stream_counter->addToReadBytes(message_len);
        external_stream_counter->addToReadCounts(new_rows);
        LOG_INFO(log, "_2");
        if (format_error)
        {
            LOG_ERROR(log, "Failed to parse message: {}", format_error.value());
            format_error.reset();
        }
        LOG_INFO(log, "_3");
        auto result_block = non_virtual_header.cloneWithColumns(format_executor->getResultColumns());
        convert_non_virtual_to_physical_action->execute(result_block);
        MutableColumns new_data(result_block.mutateColumns());
        LOG_INFO(log, "_4");
        if (!request_virtual_columns)
        {
            if (!current_batch.empty())
            {
		LOG_INFO(log, "_5");
                /// Merge all data in the current batch into the same chunk to avoid too many small chunks
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
            /// slower path
            if (!current_batch.empty())
            {
                assert(current_batch.size() == virtual_col_value_functions.size());

                /// slower path
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
                        current_batch[i]->insertMany(virtual_col_value_functions[i](message), new_rows);
                    }
                }
            }
            else
            {
                /// slower path
                for (size_t i = 0, j = 0, n = virtual_col_value_functions.size(); i < n; ++i)
                {
                    if (!virtual_col_value_functions[i])
                    {
                        /// non-virtual column: physical or calculated
                        current_batch.push_back(std::move(new_data[j]));
                        ++j;
                    }
                    else
                    {
                        auto column = virtual_col_types[i]->createColumn();
                        column->insertMany(virtual_col_value_functions[i](message), new_rows);
                        current_batch.push_back(std::move(column));
                    }
                }
            }
        }
    }
    LOG_INFO(log, "_5");
    if (!current_batch.empty())
    {
        auto rows = current_batch[0]->size();
        result_chunks.emplace_back(std::move(current_batch), rows);
    }

    LOG_INFO(log, "_6");
    iter = result_chunks.begin();
}

}

