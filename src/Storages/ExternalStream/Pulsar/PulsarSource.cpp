#include "Pulsar.h"
#include "PulsarSource.h"
#include <Processors/Executors/StreamingFormatExecutor.h>

namespace DB{

PulsarSource::PulsarSource(
    Pulsar * /*pulsar_*/,
    Block header_,
    ContextPtr query_context_,
    size_t max_block_size_,
    Poco::Logger * log_,
    ExternalStreamCounterPtr external_stream_counter_)
    : ISource(header_, true, ProcessorID::FileLogSourceID)
//    , pulsar(pulsar_)
    , query_context(query_context_)
    , max_block_size(max_block_size_)
    , log(log_)
    , column_type(header_.getByPosition(0).type)
    , external_stream_counter(external_stream_counter_)
{
    is_streaming = true;
}

PulsarSource::~PulsarSource() = default;

Chunk PulsarSource::generate()
{
    if (isCancelled())
        return {};

    if (result_chunks.empty() || iter == result_chunks.end())
    {
        readAndProcess();

        if (isCancelled())
            return {};

        if (result_chunks.empty() || iter == result_chunks.end())
            return header_chunk.clone();
    }

    return std::move(*iter++);
//    auto col = column_type->createColumn();
//    col->insertData("kartik", 6);
//    return Chunk(Columns(1, std::move(col)), 1);
}

void PulsarSource::readAndProcess()
{
    result_chunks.clear();
    current_batch.clear();
    current_batch.reserve(header.columns());

    pulsar::Messages messages;
    consumer.batchReceive(messages);

    for (const auto &messsage: messages) {
        int message_len = message.getData();
        ReadBufferFromMemory buffer(static_cast<const char *>(message.getData()), message_len);
        auto new_rows = format_executor->execute(buffer);
        external_stream_counter->addToReadBytes(message_len);
        external_stream_counter->addToReadCounts(new_rows);
        if (format_error)
        {
            LOG_ERROR(log, "Failed to parse message: {}", format_error.value());
            format_error.reset();
        }
        auto result_block = non_virtual_header.cloneWithColumns(format_executor->getResultColumns());
        convert_non_virtual_to_physical_action->execute(result_block);
        MutableColumns new_data(result_block.mutateColumns());
        if (!request_virtual_columns)
        {
            if (!current_batch.empty())
            {
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
                        current_batch[i]->insertMany(virtual_col_value_functions[i](messages), new_rows);
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
    if (!current_batch.empty())
    {
        auto rows = current_batch[0]->size();
        result_chunks.emplace_back(std::move(current_batch), rows);
    }

    iter = result_chunks.begin();
}

}
