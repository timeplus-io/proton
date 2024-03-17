#include <ClickHouse/Client.h>
#include <DataTypes/DataTypeFactory.h>
#include <Storages/ExternalTable/ClickHouse.h>
#include <ClickHouse/Sink.h>
#include <ClickHouse/Source.h>
#include <Storages/ExternalTable/ExternalTableFactory.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

namespace ExternalTable
{

ClickHouse::ClickHouse(const String & name, ExternalTableSettingsPtr settings)
    : timeouts( /// TODO do not hard-code it, allow customization via settings
        /*connection_timeout_=*/ 1 * 60 * 1'000'000,
        /*send_timeout_=*/ 1 * 60 * 1'000'000,
        /*receive_timeout_=*/ 1 * 60 * 1'000'000,
        /*tcp_keep_alive_timeout_=*/ 5 * 60 * 1'000'000
    )
    , database(settings->database.value)
    , table(settings->table.changed ? settings->table.value : name)
    , logger(&Poco::Logger::get("ExternalTable-ClickHouse-" + table))
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

    pool = DB::ConnectionPoolFactory::instance().get(
        /*max_connections=*/ 100,
        /*host=*/ host,
        /*port=*/ port,
        /*default_database=*/ settings->database.value,
        /*user=*/ settings->user.value,
        /*password=*/ settings->password.value,
        /*quota_key=*/ "",
        /*cluster=*/ "",
        /*cluster_secret=*/ "",
        "TimeplusProton",
        settings->compression.value ? Protocol::Compression::Enable : Protocol::Compression::Disable,
        settings->secure.value ? Protocol::Secure::Enable : Protocol::Secure::Disable,
        /*priority=*/ 0);
}

void ClickHouse::startup()
{
    LOG_INFO(logger, "startup");
}

Pipe ClickHouse::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo &  /*query_info*/,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t  /*max_block_size*/,
    size_t /*num_streams*/)
{
    auto header = storage_snapshot->getSampleBlockForColumns(column_names);
    auto client = std::make_unique<DB::ClickHouse::Client>(pool->get(timeouts), timeouts, logger);
    auto source = std::make_shared<DB::ClickHouse::Source>(database, table, std::move(header), std::move(client), context);
    return Pipe(std::move(source));
}

SinkToStoragePtr ClickHouse::write(const ASTPtr &  /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr  context)
{
    auto client = std::make_unique<DB::ClickHouse::Client>(pool->get(timeouts), timeouts, logger);
    return std::make_shared<DB::ClickHouse::Sink>(database, table, metadata_snapshot->getSampleBlock(), std::move(client), context, logger);
}

ColumnsDescription ClickHouse::getTableStructure()
{
    ColumnsDescription ret {};

    DB::ClickHouse::Client client = {pool->get(timeouts), timeouts, logger};
    auto query = fmt::format("DESCRIBE TABLE {}{}",
                             database.empty() ? "" : backQuoteIfNeed(database) + ".",
                             backQuoteIfNeed(table));
    /// This has to fail quickly otherwise it will block Proton from starting.
    client.executeQuery(query, /*query_id=*/"", /*fail_quick=*/true);
    LOG_INFO(logger, "Receiving table schema");
    while (true)
    {
        const auto & block = client.pollData();
        if (!block)
            break;

        auto rows = block->rows();
        if (!rows)
            continue;

        const auto & cols = block.value().getColumns();
        const auto & factory = DataTypeFactory::instance();
        for (size_t i = 0; i < rows; ++i)
        {
            ColumnDescription col_desc {};
            {
                const auto & col = block->getByName("name");
                col_desc.name = col.column->getDataAt(i).toString();
            }
            {
                const auto & col = block->getByName("type");
                col_desc.type = factory.get(col.column->getDataAt(i).toString(), /*compatible_with_clickhouse=*/true);
            }
            {
                const auto & col = block->getByName("comment");
                col_desc.comment = col.column->getDataAt(i).toString();
            }
            ret.add(col_desc, String(), false, false);
        }
    }

    client.throwServerExceptionIfAny();
    return ret;
}

}

void registerClickHouseExternalTable(ExternalTableFactory & factory)
{
    factory.registerExternalTable("clickhouse", [](const String & name, ExternalTableSettingsPtr settings)
    {
        return std::make_unique<ExternalTable::ClickHouse>(name, std::move(settings));
    });
}

}
