#include <Storages/ExternalTable/ClickHouse/ClickHouse.h>
#include "Client/Client.h"

namespace DB
{

namespace ExternalTable
{

ClickHouse::ClickHouse(ExternalTableSettingsPtr settings, ContextPtr & context_)
    : table(settings->table.value)
    , context(context_)
    , logger(&Poco::Logger::get("External-" + settings->address.value + "-" + settings->table.value))
{
    assert(settings->type.value == "clickhouse");

    auto addr = settings->address.value;
    auto pos = addr.find_first_of(':');
    if (pos == String::npos)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid ClickHouse address, expected format '<host>:<port>'");
    auto host = addr.substr(0, pos);
    auto port = std::stoi(addr.substr(pos + 1));
    if (!port)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid port in ClickHouse address");

    connection_pool = ConnectionPoolFactory::instance().get(
        100 /*max_connections*/,
        host, port,
        "default" /*default_database*/,
        "default" /*user*/,
        "" /*password*/,
        "" /*quota_key*/,
        "" /*cluster*/,
        "" /*cluster_secret*/,
         "Timeplus Proton" /*client_name*/,
        Protocol::Compression::Enable,
        Protocol::Secure::Disable,
        0 /*priority*/);

    timeouts = ConnectionTimeouts(
        10 * 60 * 1'000'000 /*connection_timeout_*/,
        10 * 60 * 1'000'000 /*send_timeout_*/,
        10 * 60 * 1'000'000 /*receive_timeout_*/
    );
}

void ClickHouse::startup()
{
    NameToNameMap map;
    auto conn = connection_pool->get(timeouts);
    Client client {conn, timeouts, context, logger};
    client.executeQuery("DESCRIBE TABLE " + table, {
        .on_data = [log = logger](Block & block)
        {
            LOG_INFO(log, "DESCRIBE TABLE returns {} columns and {} rows", block.columns(), block.rows());
            for (size_t i = 0; i < block.rows(); ++i)
            {
            }
        }
    });
}

}

}
