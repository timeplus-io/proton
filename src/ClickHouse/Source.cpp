#include <ClickHouse/Source.h>
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

Source::Source(
    const String & database,
    const String & table,
    const Block & header,
    std::unique_ptr<Client> client_,
    ContextPtr context_)
    : ISource(header, true, ProcessorID::ClickHouseSourceID)
    , client(std::move(client_))
    , query(constructSelectQuery(database, table, header))
    , context(context_)
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

    return {block->getColumns(), block->rows()};
}

}

}
