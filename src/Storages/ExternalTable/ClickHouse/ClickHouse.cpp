#include <Client/LibClient.h>
#include <DataTypes/DataTypeFactory.h>
#include <Storages/ExternalTable/ClickHouse/ClickHouse.h>
#include <Storages/ExternalTable/ClickHouse/ClickHouseSink.h>
#include <Storages/ExternalTable/ClickHouse/ClickHouseSource.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{

namespace ExternalTable
{

ClickHouse::ClickHouse(const String & name, ExternalTableSettingsPtr settings, ContextPtr &  /*context*/)
    : table(settings->table.changed ? settings->table.value : name)
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

    connection_params.host = host;
    connection_params.port = port;
    connection_params.user = settings->user.value;
    connection_params.password = settings->password.value;
    connection_params.default_database = settings->database.value;
    connection_params.security = settings->secure.value ? Protocol::Secure::Enable : Protocol::Secure::Disable;
    connection_params.timeouts = {
        /*connection_timeout_=*/ 1 * 60 * 1'000'000,
        /*send_timeout_=*/ 1 * 60 * 1'000'000,
        /*receive_timeout_=*/ 1 * 60 * 1'000'000,
        /*tcp_keep_alive_timeout_=*/ 10 * 60 * 1'000'000
    };
}

void ClickHouse::startup()
{
    LOG_INFO(logger, "startup");
}

Pipe ClickHouse::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t  /*max_block_size*/,
    size_t /*num_streams*/)
{
    auto client = std::make_unique<LibClient>(connection_params, logger);
    auto source = std::make_unique<ClickHouseSource>(std::move(client), column_names, processed_stage, context);
    return {source};
}

SinkToStoragePtr ClickHouse::write(const ASTPtr &  /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr  context)
{
    return std::make_shared<ClickHouseSink>(table, metadata_snapshot->getSampleBlock(), connection_params, context, logger);
}

ColumnsDescription ClickHouse::getTableStructure()
{
    // auto conn = std::make_unique<Connection>(
    //     connection_params.host,
    //     connection_params.port,
    //     connection_params.default_database,
    //     connection_params.user,
    //     connection_params.password,
    //     connection_params.quota_key,
    //     "", /*cluster*/
    //     "", /*cluster_secret*/
    //     "TimeplusProton",
    //     connection_params.compression,
    //     connection_params.security);
    //
    // conn->setCompatibleWithClickHouse();

    // LOG_INFO(logger, "DESCRIBE TABLE {}", table);
    // conn->sendQuery(connection_params.timeouts, "DESCRIBE TABLE " + table, {}, "", QueryProcessingStage::Complete, nullptr, nullptr, false);

    ColumnsDescription ret {};

    LibClient client {connection_params, logger};
    LOG_INFO(logger, "DESCRIBE TABLE {}", table);
    client.executeQuery("DESCRIBE TABLE " + table);
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
                col_desc.type = factory.get(col.column->getDataAt(i).toString(), true);
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

}
