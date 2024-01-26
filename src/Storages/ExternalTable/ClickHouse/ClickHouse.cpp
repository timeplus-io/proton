#include <Client/LibClient.h>
#include <DataTypes/ClickHouseDataTypeTranslator.h>
#include <DataTypes/DataTypeFactory.h>
#include <Storages/ExternalTable/ClickHouse/ClickHouse.h>
#include <Storages/ExternalTable/ClickHouse/ClickHouseSink.h>
#include <Storages/ColumnsDescription.h>

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

    connection_params.host = host;
    connection_params.port = port;
    connection_params.user = settings->user.value;
    connection_params.password = settings->password.value;
    connection_params.default_database = "default";
    connection_params.timeouts = {
        10 * 60 * 1'000'000 /*connection_timeout_*/,
        10 * 60 * 1'000'000 /*send_timeout_*/,
        10 * 60 * 1'000'000 /*receive_timeout_*/
    };
}

void ClickHouse::startup()
{
#ifdef GO_ON_PRODUCTIOn
    client.executeQuery("DESCRIBE TABLE " + table, {
        .on_data = [this](Block & block)
        {
            LOG_INFO(logger, "DESCRIBE TABLE returns {} columns and {} rows", block.columns(), block.rows());
            auto cols = block.getColumns();
            for (size_t i = 0; i < block.rows(); ++i)
            {
                String msg = "row " + std::to_string(i) + " :";
                for (const auto & col : cols)
                {
                  msg += col->getName() + ": ";
                  msg += (*col)[i].getTypeName();
                }
            }
        }
    });
#endif
    LOG_INFO(logger, "startup");
}

SinkToStoragePtr ClickHouse::write(const ASTPtr &  /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr  /*context*/)
{
    return std::make_shared<ClickHouseSink>(metadata_snapshot->getSampleBlock(), logger);
}

ColumnsDescription ClickHouse::getTableStructure()
{
    auto conn = std::make_shared<Connection>(
        connection_params.host,
        connection_params.port,
        connection_params.default_database,
        connection_params.user,
        connection_params.password,
        connection_params.quota_key,
        "", /*cluster*/
        "", /*cluster_secret*/
        "TimeplusProton",
        connection_params.compression,
        connection_params.security);

    conn->setDataTypeTranslator(&ClickHouseDataTypeTranslator::instance());

    LOG_INFO(logger, "executing SQL: DESCRIBE TABLE {}", table);
    conn->sendQuery(connection_params.timeouts, "DESCRIBE TABLE " + table, {}, "", QueryProcessingStage::Complete, nullptr, nullptr, false);
    LOG_INFO(logger, "receiving data");

    ColumnsDescription ret {};

    LibClient client {std::move(conn), connection_params.timeouts, context, logger};
    client.receiveResult({
        .on_data = [this, &ret](Block & block)
        {
            LOG_INFO(logger, "DESCRIBE TABLE returns {} columns and {} rows", block.columns(), block.rows());
            if (!block.rows())
                return;

            const auto & cols = block.getColumns();
            const auto & factory = DataTypeFactory::instance();
            for (size_t i = 0; i < block.rows(); ++i)
            {
                ColumnDescription col_desc {};
                {
                    const auto & col = block.getByName("name");
                    col_desc.name = col.column->getDataAt(i).toString();
                }
                {
                    const auto & col = block.getByName("type");
                    col_desc.type = factory.get(col.column->getDataAt(i).toString(), true);
                }
                {
                    const auto & col = block.getByName("comment");
                    col_desc.comment = col.column->getDataAt(i).toString();
                }
                LOG_INFO(logger, "row {}: col_name = {}, col_type = {}", i, col_desc.name, col_desc.type);
                ret.add(col_desc, String(), false, false);
            }
        }
    });

    return ret;
}

}

}
