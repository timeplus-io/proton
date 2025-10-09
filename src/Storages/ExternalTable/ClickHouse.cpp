#include <Storages/ExternalTable/ClickHouse.h>

#include <ClickHouse/Client.h>
#include <ClickHouse/Sink.h>
#include <ClickHouse/Source.h>
#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ParserTablesInSelectQuery.h>
#include <Parsers/parseQuery.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ExternalTable/ExternalTableFactory.h>
#include <Storages/MatView/StorageMaterializedView.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NO_FREE_CONNECTION;
}

namespace ExternalTable
{

ClickHouse::ClickHouse(
    const StorageID & table_id,
    const StorageInMemoryMetadata & storage_metadata,
    std::unique_ptr<ExternalTableSettings> settings_,
    bool attach_,
    const ContextPtr & context_)
    : StorageExternalTable(table_id, storage_metadata, std::move(settings_), attach_, context_)
    , timeouts(ConnectionTimeouts::getTCPTimeoutsWithoutFailover(context_->getSettingsRef()))
    , database(settings->database.value)
    , table(settings->table.changed ? settings->table.value : table_id.getTableName())
    , logger(getLogger("ExternalTable-ClickHouse-" + table_id.getFullTableName()))
{
    assert(settings->type.value == "clickhouse");

    auto addr = settings->address.value;
    auto pos = addr.find_first_of(':');
    if (pos == String::npos)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid ClickHouse address, expected format '<host>:<port>'");
    auto host = addr.substr(0, pos);
    auto port = std::stoi(addr.substr(pos + 1));
    if (port == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid port in ClickHouse address");

    auto max_connections = settings->pooled_connections.changed ? settings->pooled_connections : DEFAULT_CONNECTION_POOL_SIZE;
    pool = DB::ClickHouse::ConnectionPoolFactory::instance().get(
        /*max_connections=*/static_cast<uint32_t>(max_connections),
        /*host=*/host,
        /*port=*/port,
        /*default_database=*/settings->database.value,
        /*user=*/settings->user.value,
        /*password=*/settings->password.value,
        /*quota_key=*/"",
        /*cluster=*/"",
        /*cluster_secret=*/"",
        "Proton",
        settings->compression.value ? Protocol::Compression::Enable : Protocol::Compression::Disable,
        settings->secure.value ? Protocol::Secure::Enable : Protocol::Secure::Disable,
        /*priority=*/0,
        /*ssl_verify_mode=*/settings->ssl_verify_mode.value,
        /*ssl_ca_cert_file=*/settings->ssl_ca_cert_file.value,
        /*ssl_cert_file=*/settings->ssl_cert_file.value,
        /*ssl_key_file=*/settings->ssl_key_file.value);

    if (!attach_)
    {
        Settings settings;
        settings.connection_pool_max_wait_ms = timeouts.connection_timeout.milliseconds();
        /// validate connection
        std::ignore = pool->get(timeouts, &settings, true);
    }
}

Int64 ClickHouse::getMetricTime(bool reset)
{
    if (reset)
        return metrics.metric_time.exchange(MonotonicMilliseconds::now(), std::memory_order_relaxed);
    else
        return metrics.metric_time.load(std::memory_order_relaxed);
}

UInt64 ClickHouse::readBytes(bool reset, bool /*external_ingress*/)
{
    if (reset)
        return metrics.public_read_bytes.exchange(0, std::memory_order_relaxed);
    else
        return metrics.public_read_bytes.load(std::memory_order_relaxed);
}

UInt64 ClickHouse::readRows(bool reset, bool /*external_ingress*/)
{
    if (reset)
        return metrics.public_read_rows.exchange(0, std::memory_order_relaxed);
    else
        return metrics.public_read_rows.load(std::memory_order_relaxed);
}

UInt64 ClickHouse::writtenBytes(bool reset, bool /*external_ingress*/)
{
    if (reset)
        return metrics.public_written_bytes.exchange(0, std::memory_order_relaxed);
    else
        return metrics.public_written_bytes.load(std::memory_order_relaxed);
}

UInt64 ClickHouse::writtenRows(bool reset, bool /*external_ingress*/)
{
    if (reset)
        return metrics.public_written_rows.exchange(0, std::memory_order_relaxed);
    else
        return metrics.public_written_rows.load(std::memory_order_relaxed);
}

Pipe ClickHouse::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr local_context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    size_t /*num_streams*/)
{
    auto header = storage_snapshot->getSampleBlockForColumns(column_names);
    auto client = createClient(local_context, /*use_dedicated_connection=*/true);
    auto source = std::make_shared<DB::ClickHouse::Source>(database, table, std::move(header), std::move(client));
    source->setProgressCallback([this](const Progress & progress) {
        const auto & value = progress.getValues();
        this->metrics.public_read_bytes += value.read_bytes;
        this->metrics.public_read_rows += value.read_rows;
    });
    return Pipe(std::move(source));
}

SinkToStoragePtr ClickHouse::writeImpl(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context)
{
    /// If the query (or MV that sends data to this) does not explicitely insert data into a column,
    /// then we don't put that column in the INSERT query, which will be sent to CH.
    /// In this way, we will let CH to compute the default value for such column.
    auto maybe_sample_block = getWriteSampleBlock(query, metadata_snapshot, local_context);
    Names columns_to_send
        = maybe_sample_block.has_value() ? maybe_sample_block->getNames() : metadata_snapshot->getSampleBlock().getNames();

    auto client = createClient(local_context, /*use_dedicated_connection=*/false);
    auto sink = std::make_shared<DB::ClickHouse::Sink>(
        database, table, metadata_snapshot->getSampleBlock(), columns_to_send, std::move(client), local_context, logger);
    sink->setProgressCallback([this](const Progress & progress) {
        const auto & value = progress.getValues();
        this->metrics.public_written_bytes += value.written_bytes;
        this->metrics.public_written_rows += value.written_rows;
    });
    return sink;
}

void ClickHouse::getTableSchema(ContextPtr local_context, ColumnsDescription & desc)
{
    auto client = createClient(local_context, /*use_dedicated_connection=*/true);
    auto query = fmt::format("DESCRIBE TABLE {}{}", database.empty() ? "" : backQuoteIfNeed(database) + ".", backQuoteIfNeed(table));
    /// This has to fail quickly otherwise it will block Proton from starting.
    client->executeQuery(query, /*query_id=*/"", /*fail_quick=*/true);
    LOG_INFO(logger, "Receiving table schema");
    while (true)
    {
        const auto & block = client->pollData();
        if (!block)
            break;

        auto rows = block->rows();
        if (rows == 0u)
            continue;

        const auto & cols = block.value().getColumns();
        const auto & factory = DataTypeFactory::instance();
        for (size_t i = 0; i < rows; ++i)
        {
            ColumnDescription col_desc{};
            {
                const auto & col = block->getByName("name");
                col_desc.name = col.column->getDataAt(i).toString();
            }
            {
                const auto & col = block->getByName("type");
                col_desc.type = factory.get(col.column->getDataAt(i).toString(), /*compatible_with_clickhouse=*/true);

                original_column_type_names[col_desc.name] = col.column->getDataAt(i).toString();
            }
            {
                const auto & col = block->getByName("comment");
                col_desc.comment = col.column->getDataAt(i).toString();
            }
            desc.add(col_desc, String(), false, false);
        }
    }

    client->throwServerExceptionIfAny();
}

std::unique_ptr<DB::ClickHouse::Client> ClickHouse::createClient(const ContextPtr & local_context, bool use_dedicated_connection) const
{
    auto this_clickhouse_ptr = std::dynamic_pointer_cast<const ClickHouse>(shared_from_this());
    assert(this_clickhouse_ptr);

    const auto & settings = local_context->getSettingsRef();
    if (use_dedicated_connection)
    {
        auto connection = pool->get(timeouts, &settings, true);
        if (!connection)
            throw Exception(
                ErrorCodes::NO_FREE_CONNECTION,
                "No connection available, please try stop some running queries or use a bigger value for pooled_connections setting");
        return std::make_unique<DB::ClickHouse::Client>(this_clickhouse_ptr, *connection, timeouts, logger);
    }
    else
    {
        return std::make_unique<DB::ClickHouse::Client>(
            this_clickhouse_ptr,
            pool,
            /*connection_pool_max_wait_ms=*/settings.connection_pool_max_wait_ms.totalMilliseconds(),
            timeouts,
            logger);
    }
}
}

void registerClickHouseExternalTable(ExternalTableFactory & factory)
{
    factory.registerExternalTable(
        "clickhouse",
        [](const StorageID & table_id,
           const StorageInMemoryMetadata & storage_metadata,
           std::unique_ptr<ExternalTableSettings> settings,
           bool attach,
           ContextPtr context) -> StoragePtr {
            return std::make_shared<ExternalTable::ClickHouse>(table_id, storage_metadata, std::move(settings), attach, context);
        });
}

}
