#include <Processors/Executors/MessageQueueFormatExecutor.h>

#include <Formats/FormatFactory.h>
#include <Processors/Formats/IRowOutputFormat.h>
#include <Processors/ProcessorID.h>

namespace DB
{

MessageQueueFormatExecutor::MessageQueueFormatExecutor(
    const Block & header_,
    const String & format_name_,
    const FormatSettings & format_settings_,
    size_t max_rows_per_message_,
    size_t max_message_size_,
    const ContextPtr & context_)
    : header(header_)
    , format_name(format_name_)
    , format_settings(format_settings_)
    , max_rows_per_message(max_rows_per_message_)
    , max_message_size(max_message_size_)
    , context(context_)
{
    format_settings.protobuf.allow_multiple_rows_without_delimiter = true;
}

void MessageQueueFormatExecutor::start()
{
    buffer = std::make_unique<WriteBufferFromOwnString>();
    format = FormatFactory::instance().getOutputFormat(format_name, *buffer, header, context, format_settings);
    row_format = dynamic_cast<IRowOutputFormat *>(format.get());
}

void MessageQueueFormatExecutor::finish()
{
}

void MessageQueueFormatExecutor::execute(const Block & block, MessageCallback message_callback, bool flush_at_the_end)
{
    const auto & columns = block.getColumns();
    if (columns.empty())
        return;

    assert(format != nullptr);

    auto rows_in_block = block.rows();
    if (row_format != nullptr)
    {
        size_t row = 0;
        bool flushed{false};
        while (row < rows_in_block)
        {
            flushed = false;
            row_format->writePrefixIfNeeded();
            size_t i = 0;
            for (; accumulated_rows < max_rows_per_message && row < rows_in_block
                 && (max_message_size == 0 || accumulated_size + buffer->stringRef().size < max_message_size);
                 ++i, ++row, ++accumulated_rows)
            {
                if (i != 0)
                    row_format->writeRowBetweenDelimiter();
                row_format->writeRow(columns, row);
            }

            if (accumulated_rows >= max_rows_per_message || accumulated_size + buffer->stringRef().size >= max_message_size)
            {
                doFlush(message_callback);
                flushed = true;
            }
            else
            {
                accumulated_size += buffer->stringRef().size;
            }
        }

        if (!flushed && flush_at_the_end)
            doFlush(std::move(message_callback));
    }
    else
    {
        format->write(block);
        format->finalize();
        message_callback(buffer->str(), rows_in_block);
        format->resetFormatter();
        buffer->restart();
    }
}

void MessageQueueFormatExecutor::flush(MessageCallback message_callback)
{
    if (accumulated_rows == 0) /// nothing to flush
        return;

    doFlush(std::move(message_callback));
}

void MessageQueueFormatExecutor::doFlush(MessageCallback message_callback)
{
    row_format->finalize();
    row_format->resetFormatter();
    message_callback(buffer->str(), accumulated_rows);

    /// Reallocate buffer if it's capacity is large then DBMS_DEFAULT_BUFFER_SIZE,
    /// because most likely in this case we serialized abnormally large row
    /// and won't need this large allocated buffer anymore.
    buffer->restart(DBMS_DEFAULT_BUFFER_SIZE);

    accumulated_rows = 0;
    accumulated_size = 0;
}

}
