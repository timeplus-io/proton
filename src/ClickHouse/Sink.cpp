#include <ClickHouse/Sink.h>
#include <Client/ConnectionParameters.h>
#include <Processors/Formats/IOutputFormat.h>

namespace DB
{

namespace ClickHouse
{

namespace
{

String constructInsertQuery(const String & database, const String & table, const Block & header)
{
    assert(header.columns());
    const auto & col_names = header.getNames();

    auto query = "INSERT INTO " + (database.empty() ? "" : backQuoteIfNeed(database) + ".") + backQuoteIfNeed(table) + " (" + backQuoteIfNeed(col_names[0]);
    for (const auto & name : std::vector<String>(std::next(col_names.begin()), col_names.end()))
        query.append(", " + backQuoteIfNeed(name));
    query.append(") VALUES ");

    return query;
}

}

Sink::Sink(
    const String & database,
    const String & table,
    const Block & header,
    std::unique_ptr<Client> client_,
    ContextPtr context_,
    Poco::Logger * logger_)
    : SinkToStorage(header, ProcessorID::ExternalTableDataSinkID)
    , insert_into(constructInsertQuery(database, table, header))
    , client(std::move(client_))
    , context(context_)
    , logger(logger_)
{
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

void Sink::consume(Chunk chunk)
{
    if (!chunk.rows())
        return;

    BufferResetter reset_buffer(*buf); /// makes sure buf gets reset afterwards
    buf->write(insert_into.data(), insert_into.size());
    auto block = getHeader().cloneWithColumns(chunk.detachColumns());
    output_format->write(block);

    client->executeInsertQuery(buf->str());
    client->throwServerExceptionIfAny();
}

}

}
