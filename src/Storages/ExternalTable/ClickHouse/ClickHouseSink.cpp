#include <Client/ConnectionParameters.h>
#include <Storages/ExternalTable/ClickHouse/ClickHouseSink.h>
#include <Client/LibClient.h>
#include <Formats/FormatFactory.h>
#include <Processors/Formats/IOutputFormat.h>
#include "Interpreters/Context.h"

namespace DB
{

namespace ExternalTable
{

ClickHouseSink::ClickHouseSink(
        const String & table,
        const Block & header,
        const ConnectionParameters & params_,
        ContextPtr & context_,
        Poco::Logger * logger_)
    : SinkToStorage(header, ProcessorID::ExternalTableDataSinkID)
    , params(params_)
    , context(context_)
    , logger(logger_)
{
    const auto & col_names = header.getNames();
    assert(!col_names.empty());

    insert_into = "INSERT INTO " + backQuoteIfNeed(table) + " (" + backQuoteIfNeed(col_names[0]);
    for (const auto & name : std::vector<String>(std::next(col_names.begin()), col_names.end()))
        insert_into.append(", " + backQuoteIfNeed(name));
    insert_into.append(") VALUES ");

    conn = std::make_unique<Connection>(
        params.host,
        params.port,
        params.default_database,
        params.user,
        params.password,
        params.quota_key,
        "", /*cluster*/
        "", /*cluster_secret*/
        "TimeplusProton",
        params.compression,
        params.security);

    conn->setCompatibleWithClickHouse();

    // buf = std::make_unique<WriteBufferFromOStream>(oss);
    buf = std::make_unique<WriteBufferFromOwnString>();
    auto format_settings = getFormatSettings(context);
    format_settings.values.no_commas_between_rows = true;
    output_format = FormatFactory::instance().getOutputFormat("Values", *buf, header, context, {}, format_settings);
    output_format->setAutoFlush();

    LOG_INFO(logger, "ClickHouseSink is read to send data to table {}", table);
}

namespace
{

class BufferResetter
{
public:
explicit BufferResetter(WriteBufferFromOwnString & buf_): buf(buf_) {}
~BufferResetter() { buf.restart(); }

private:
    WriteBufferFromOwnString & buf;
};

}

void ClickHouseSink::consume(Chunk chunk)
{
    /// Empty chunks are acting heartbeats
    if (!chunk.rows())
    {
        conn->checkConnected(); /// ping to keep connection alive
        return;
    }

    LOG_INFO(logger, "consuming from chunk contains {} rows", chunk.rows());

    BufferResetter reset_buffer(*buf); /// makes sure buf gets reset afterwards
    buf->write(insert_into.data(), insert_into.size());
    auto block = getHeader().cloneWithColumns(chunk.detachColumns());
    output_format->write(block);

    String query_to_sent {buf->buffer().begin(), buf->offset()};
    LOG_INFO(logger, "sending query {}", query_to_sent);
    conn->sendQuery(params.timeouts, query_to_sent, {}, "", QueryProcessingStage::Complete, nullptr, nullptr, false);
    LOG_INFO(logger, "query sent!");

    LibClient client {*conn, params.timeouts, logger};
    client.receiveResult({
        .on_data = [this](Block & block_)
        {
            LOG_INFO(logger, "INSERT INTO returns {} columns and {} rows", block_.columns(), block_.rows());
            if (!block_.rows())
                return;

            const auto & cols = block_.getColumns();
            for (size_t i = 0; i < block_.rows(); ++i)
            {
                for (const auto & col : block_.getColumns())
                LOG_INFO(logger, "row {}: col_name = {}", i, col->getName());
            }
        }
    });
    client.throwServerExceptionIfAny();
    LOG_INFO(logger, "consume done!");
}

}

}
