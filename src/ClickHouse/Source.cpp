#include <ClickHouse/Source.h>

#include <QueryPipeline/RemoteQueryExecutor.h>
#include <Common/quoteString.h>

namespace DB
{

namespace ClickHouse
{

namespace
{
String constructSelectQuery(const String & database, const String & table, const Block & header)
{
    assert(header.columns());
    const auto & col_names = header.getNames();

    auto query = "SELECT " + backQuoteIfNeed(col_names[0]);
    for (const auto & name : std::vector<String>(std::next(col_names.begin()), col_names.end()))
        query.append(", " + backQuoteIfNeed(name));
    query.append(" FROM " + (database.empty() ? "" : backQuoteIfNeed(database) + ".") + backQuoteIfNeed(table));

    return query;
}

}

Source::Source(const String & database, const String & table, const Block & header, std::unique_ptr<Client> client_)
    : ISource(header, true, ProcessorID::ClickHouseSourceID)
    , chunk_header(header.cloneEmptyColumns(), 0)
    , client(std::move(client_))
    , query(constructSelectQuery(database, table, header))
{
}

Source::Source(const String & query_, const Block & header, std::unique_ptr<Client> client_)
    : ISource(header, true, ProcessorID::ClickHouseSourceID)
    , chunk_header(header.cloneEmptyColumns(), 0)
    , client(std::move(client_))
    , query(query_)
{
}

Chunk Source::generate()
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

    auto block = client->pollData();
    client->throwServerExceptionIfAny();
    if (!block)
        return {};

    if (block->columns() > 0)
    {
        if (!need_conversion.has_value())
            need_conversion = !blocksHaveEqualStructure(*block, getPort().getHeader());

        if (need_conversion.value())
            block = RemoteQueryExecutor::adaptBlockStructure(*block, getPort().getHeader());

        return {block->getColumns(), block->rows()};
    }
    else
    {
        /// When there are no columns, return chunk with zero rows (chunk header)
        /// instead of empty chunk which will terminate the query prematurely
        return chunk_header.clone();
    }
}

}

}
