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
    // if (isCancelled())
    // {
    // }

    /// TODO re-design the client API to provide a function to poll data instead of using callbacks.
    client->executeQuery(query, {
        .on_data = [](Block & blk)
        {
        }
    });
    return {};
}

}

}
