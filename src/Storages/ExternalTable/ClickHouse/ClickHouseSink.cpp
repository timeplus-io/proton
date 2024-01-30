#include <Client/ConnectionParameters.h>
#include <Client/LibClient.h>
#include <Formats/FormatFactory.h>
#include <Interpreters/Context.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Storages/ExternalTable/ClickHouse/ClickHouseSink.h>

namespace DB
{

namespace ExternalTable
{

namespace
{

String constructInsertQuery(const String & table, const Block & header)
{
    assert(header.columns());
    const auto & col_names = header.getNames();

    auto query = "INSERT INTO " + backQuoteIfNeed(table) + " (" + backQuoteIfNeed(col_names[0]);
    for (const auto & name : std::vector<String>(std::next(col_names.begin()), col_names.end()))
        query.append(", " + backQuoteIfNeed(name));
    query.append(") VALUES ");

    return query;
}

}

ClickHouseSink::ClickHouseSink(
        const String & table,
        const Block & header,
        const ConnectionParameters & params_,
        ContextPtr context_,
        Poco::Logger * logger_)
    : SinkToStorage(header, ProcessorID::ExternalTableDataSinkID)
    , insert_into(constructInsertQuery(table, header))
    , params(params_)
    , context(context_)
    , logger(logger_)
{
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

    buf = std::make_unique<WriteBufferFromOwnString>();
    auto format_settings = getFormatSettings(context);
    format_settings.values.no_commas_between_rows = true;
    output_format = FormatFactory::instance().getOutputFormat("Values", *buf, header, context, {}, format_settings);
    output_format->setAutoFlush();

    LOG_INFO(logger, "ready to send data to ClickHouse table {} with {}", table, insert_into);
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
    if (!chunk.rows())
        return;

    BufferResetter reset_buffer(*buf); /// makes sure buf gets reset afterwards
    buf->write(insert_into.data(), insert_into.size());
    auto block = getHeader().cloneWithColumns(chunk.detachColumns());
    output_format->write(block);

    String query_to_sent {buf->str()};
    conn->forceConnected(params.timeouts); /// The connection chould have been idle for too long
    conn->sendQuery(params.timeouts, query_to_sent, {}, "", QueryProcessingStage::Complete, nullptr, nullptr, false);

    LibClient client {*conn, params.timeouts, logger};
    client.receiveResult();
    client.throwServerExceptionIfAny();
}

}

}
