#include <Common/quoteString.h>
#include <Storages/ExternalTable/ClickHouse/ClickHouseSource.h>

namespace DB
{

namespace ExternalTable
{

namespace
{
String constructSelectQuery(const String & table, const Block & header)
{
    assert(header.columns());
    const auto & col_names = header.getNames();

    auto query = "SELECT " + backQuoteIfNeed(col_names[0]);
    for (const auto & name : std::vector<String>(std::next(col_names.begin()), col_names.end()))
        query.append(", " + backQuoteIfNeed(name));
    query.append(" FROM " + table);

    return query;
}

}

ClickHouseSource::ClickHouseSource(
    const String & table,
    const Block & header,
    std::unique_ptr<LibClient> client_,
    QueryProcessingStage::Enum /*processed_stage*/,
    ContextPtr context_,
    Poco::Logger * logger_)
    : ISource(header, true, ProcessorID::ClickHouseSourceID)
    , client(std::move(client_))
    , query(constructSelectQuery(table, header))
    , context(context_)
    , logger(logger_)
{
}

Chunk ClickHouseSource::generate()
{
    if (isCancelled())
    {
        if (started)
            client->cancelQuery();

        return {};
    }

    if (!started)
    {
        started = true;
        client->executeQuery(query);
    }

    LOG_INFO(logger, "polling data");
    auto block = client->pollData();
    client->throwServerExceptionIfAny();
    if (!block)
    {
        LOG_INFO(logger, "no more data");
        return {};
    }

    LOG_INFO(logger, "received {} rows", block->rows());
    return {block->getColumns(), block->rows()};
}

}

}
