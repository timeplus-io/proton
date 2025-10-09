#pragma once

#include <Interpreters/Context.h>

namespace DB
{

class IOutputFormat;
class IRowOutputFormat;
using IOutputFormatPtr = std::shared_ptr<IOutputFormat>;


/// MessageQueueFormatExecutor implements exactly the same logic as MessageQueueSink, but implemented as an executor instead of a sink.
class MessageQueueFormatExecutor
{
public:
    using MessageCallback = std::function<void(const String & message, size_t rows_in_message)>;

    MessageQueueFormatExecutor(
        const Block & header_,
        const String & format_name_,
        const FormatSettings & format_settings_,
        size_t max_rows_per_message_,
        size_t max_message_size_,
        const ContextPtr & context_);

    void execute(const Block &, MessageCallback message_callback, bool flush_at_the_end = false);

    void start();
    void finish();
    void flush(MessageCallback message_callback);

private:
    void doFlush(MessageCallback message_callback);

    const Block header;
    const String format_name;
    FormatSettings format_settings;

    size_t max_rows_per_message{0};
    size_t max_message_size{0};

    size_t accumulated_size{0};
    size_t accumulated_rows{0};

    std::unique_ptr<WriteBufferFromOwnString> buffer;
    IOutputFormatPtr format;
    IRowOutputFormat * row_format;

    const ContextPtr context;
};

}
