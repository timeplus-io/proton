#include <Storages/ExternalStream/Iceberg/Iceberg.h>

#if USE_AWS_S3 && USE_AVRO && USE_PARQUET

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <Databases/ApacheIceberg/DatabaseIceberg.h>
#include <Formats/Avro/InputStreamReadBufferAdapter.h>
#include <Formats/Avro/OutputStreamWriteBufferAdapter.h>
#include <IO/ReadBufferFromS3.h>
#include <Processors/Sources/NullSource.h>
#include <Storages/ExternalStream/Iceberg/IcebergSink.h>
#include <Storages/ExternalStream/Iceberg/IcebergSource.h>
#include <Storages/Iceberg/AvroSchemas.h>
#include <Storages/Iceberg/Schema.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/getVirtualsForStorage.h>

#include <Compiler.hh>
#include <DataFile.hh>
#include <Stream.hh>

namespace DB
{

namespace ErrorCodes
{
extern const int ICEBERG_CATALOG_ERROR;
extern const int ILLEGAL_COLUMN;
extern const int INVALID_SETTING_VALUE;
extern const int UNSUPPORTED;
}

namespace
{

void updateFormatFactorySettings(FormatFactorySettings & settings, const ContextPtr & context)
{
    const auto & changes = context->getSettingsRef().changes();
    for (const auto & change : changes)
    {
        if (settings.has(change.name))
            settings.set(change.name, change.value);
    }
}

}

namespace ExternalStream
{

void Iceberg::prepareS3Configuration(const ContextPtr & context)
{
    auto storage_endpoint = settings->iceberg_storage_endpoint.value;
    if (auto table_metadata = tryGetTableMetadata(); !table_metadata.getTableUUID().empty())
        storage_endpoint = table_metadata.getLocation(/*path_only=*/false);

    if (storage_endpoint.empty())
        throw Exception(ErrorCodes::ICEBERG_CATALOG_ERROR, "Storage endpoint was empty");

    IcebergS3Configuration configuration;

    configuration.url = S3::URI(storage_endpoint);
    configuration.format = "Parquet";

    configuration.min_upload_file_size = context->getSettingsRef().s3_min_upload_file_size;
    configuration.max_upload_idle_seconds = context->getSettingsRef().s3_max_upload_idle_seconds;

    configuration.auth_settings.access_key_id = settings->access_key_id;
    configuration.auth_settings.secret_access_key = settings->secret_access_key;
    configuration.auth_settings.session_token = settings->session_token;
    configuration.auth_settings.use_environment_credentials = settings->use_environment_credentials;

    configuration.request_settings.updateFromSettings(context->getSettingsRef());

    s3_configuration = std::move(configuration);
    s3_configuration.createClient(context);
}

void Iceberg::calculateVirtualColumns()
{
    auto default_virtuals = NamesAndTypesList{
        {"_path", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())},
        {"_file", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())}};

    auto columns = getInMemoryMetadata().getSampleBlock().getNamesAndTypesList();
    virtual_columns = getVirtualsForStorage(columns, default_virtuals);
    for (const auto & column : virtual_columns)
        virtual_block.insert({column.type->createColumn(), column.type, column.name});
}

Iceberg::Iceberg(
    StorageID storage_id,
    StorageInMemoryMetadata metadata,
    ExternalStreamSettingsPtr settings_,
    ExternalStreamCounterPtr,
    ContextPtr context)
    : StorageExternalStreamImpl(std::move(storage_id), std::move(metadata), std::move(settings_), context)
{
    prepareS3Configuration(context);
    calculateVirtualColumns();
}

Apache::Iceberg::CatalogPtr Iceberg::getCatalog() const
{
    auto database = DatabaseCatalog::instance().getDatabase(getStorageID().database_name);
    auto iceberg_db = std::dynamic_pointer_cast<DatabaseApacheIceberg>(database);
    /// For now, we only allow creating Iceberg external stream in an Iceberg database.
    /// The restriction will be removed later.
    if (!iceberg_db)
        throw Exception(ErrorCodes::UNSUPPORTED, "Iceberg external stream can only be created in an Iceberg database");

    return iceberg_db->getCatalog();
}

Apache::Iceberg::TableMetadata Iceberg::getTableMetadata() const
{
    auto storage_id = getStorageID();
    /// TODO withCredentials()
    Apache::Iceberg::TableMetadata metadata;
    metadata.withSchema().withLocation();
    getCatalog()->getTableMetadata(storage_id.database_name, storage_id.table_name, metadata);

    return metadata;
}

Apache::Iceberg::TableMetadata Iceberg::tryGetTableMetadata() const
{
    auto storage_id = getStorageID();
    /// TODO withCredentials()
    Apache::Iceberg::TableMetadata metadata;
    metadata.withSchema().withLocation();
    getCatalog()->tryGetTableMetadata(storage_id.database_name, storage_id.table_name, metadata);

    return metadata;
}

std::list<Apache::Iceberg::ManifestList> Iceberg::fetchManifestList(const Apache::Iceberg::TableMetadata & table_metadata) const
{
    std::list<Apache::Iceberg::ManifestList> manifest_lists;

    const auto & manifest_list_uri = table_metadata.getManifestList();
    if (manifest_list_uri.empty())
    {
        LOG_INFO(logger, "Table metadata does not have a manifest list.");
        return manifest_lists;
    }

    auto manifest_object = S3::URI(manifest_list_uri);

    ReadBufferFromS3 buf{
        s3_configuration.client,
        manifest_object.bucket,
        manifest_object.key,
        manifest_object.version_id,
        s3_configuration.request_settings,
        /*settings_=*/{}};

    auto is = std::make_unique<Avro::InputStreamReadBufferAdapter>(buf);

    /// It's possible that the data schema does not match our MANIFEST_LIST schema.
    /// auto schema = avro::compileJsonSchemaFromString(Apache::Iceberg::AvroSchemas::MANIFEST_LIST);
    /// avro::DataFileReader<Apache::Iceberg::ManifestList> reader{std::move(is), schema};
    avro::DataFileReader<Apache::Iceberg::ManifestList> reader{std::move(is)};

    Apache::Iceberg::ManifestList manifest_list;
    while (reader.read(manifest_list))
    {
        LOG_INFO(logger, "Got manifest_list sn = {} sid = {}", manifest_list.sequence_number, manifest_list.added_snapshot_id);
        manifest_lists.push_back(std::move(manifest_list));
    }

    return manifest_lists;
}

std::optional<UInt64> Iceberg::totalRows(const Settings &) const
{
    auto table_metadata = getTableMetadata();
    if (const auto & total_records = table_metadata.getCurrentSnapshot().summary.total_records; !total_records.empty())
        return {std::stoll(total_records)};
    else
        return std::nullopt;
}

namespace
{
std::shared_ptr<IcebergSource::IIterator> createFileIterator(
    std::list<Apache::Iceberg::ManifestList> manifest_list,
    IcebergS3Configuration s3_configuration,
    LoggerPtr logger,
    std::function<void(FileProgress)> file_progress_callback = {})
{
    return std::make_shared<IcebergSource::KeysIterator>(std::move(manifest_list), s3_configuration, logger, file_progress_callback);
}
}

Pipe Iceberg::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr local_context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t num_streams)
{
    auto table_metadata = getTableMetadata();
    auto manifest_lists = fetchManifestList(table_metadata);

    auto header = storage_snapshot->getSampleBlockForColumns(column_names);

    Pipes pipes;

    if (manifest_lists.empty())
    { /// No data in the table
        LOG_INFO(logger, "No manifest list, no data to read");
        pipes.reserve(1);
        pipes.emplace_back(std::make_shared<NullSource>(header));
    }
    else
    {
        std::unordered_set<String> column_names_set(column_names.begin(), column_names.end());
        std::vector<NameAndTypePair> requested_virtual_columns;

        for (const auto & virtual_column : getVirtuals())
        {
            if (column_names_set.contains(virtual_column.name))
                requested_virtual_columns.push_back(virtual_column);
        }

        std::shared_ptr<IcebergSource::IIterator> iterator_wrapper = createFileIterator(manifest_lists, s3_configuration, logger);

        ColumnsDescription columns_description;
        Block block_for_format;
        if (supportsSubsetOfColumns(local_context))
        {
            auto fetch_columns = column_names;
            const auto & virtuals = getVirtuals();
            std::erase_if(fetch_columns, [&](const String & col) {
                return std::any_of(
                    virtuals.begin(), virtuals.end(), [&](const NameAndTypePair & virtual_col) { return col == virtual_col.name; });
            });

            if (fetch_columns.empty())
                fetch_columns.push_back(ExpressionActions::getSmallestColumn(storage_snapshot->metadata->getColumns().getAllPhysical()));

            columns_description = storage_snapshot->getDescriptionForColumns(fetch_columns);
            block_for_format = storage_snapshot->getSampleBlockForColumns(columns_description.getNamesOfPhysical());
        }
        else
        {
            columns_description = storage_snapshot->metadata->getColumns();
            block_for_format = storage_snapshot->metadata->getSampleBlock();
        }

        const size_t max_download_threads = local_context->getSettingsRef().max_download_threads;
        for (size_t i = 0; i < num_streams; ++i)
        {
            pipes.emplace_back(std::make_shared<IcebergSource>(
                requested_virtual_columns,
                "Parquet", /// for now, only parquet data files are supported
                getName(),
                block_for_format,
                local_context,
                getFormatSettings(local_context),
                columns_description,
                max_block_size,
                /*compression_method=*/"none",
                s3_configuration,
                iterator_wrapper,
                max_download_threads));
        }
    }

    auto pipe = Pipe::unitePipes(std::move(pipes));

    return pipe;
}

SinkToStoragePtr Iceberg::write(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context)
{
    auto table_metadata = getTableMetadata();
    auto manifest_lists = fetchManifestList(table_metadata);

    auto min_upload_file_size_ = local_context->getSettingsRef().s3_min_upload_file_size.changed
        ? local_context->getSettingsRef().s3_min_upload_file_size.value
        : s3_configuration.min_upload_file_size;

    auto max_upload_idle_seconds_ = local_context->getSettingsRef().s3_max_upload_idle_seconds.changed
        ? local_context->getSettingsRef().s3_max_upload_idle_seconds.value
        : s3_configuration.max_upload_idle_seconds;

    auto sample_block = metadata_snapshot->getSampleBlock();

    auto format_settings = getFormatSettings(local_context);

    return std::make_shared<IcebergSink>(
        getStorageID(),
        "Parquet",
        sample_block,
        format_settings,
        s3_configuration,
        min_upload_file_size_,
        max_upload_idle_seconds_,
        std::move(table_metadata),
        std::move(manifest_lists),
        getCatalog(),
        local_context);
}

FormatSettings Iceberg::getFormatSettings(const ContextPtr & local_context) const
{
    FormatFactorySettings settings = format_factory_settings.has_value() ? *format_factory_settings : FormatFactorySettings();
    updateFormatFactorySettings(settings, local_context);
    auto format_settings = DB::getFormatSettings(local_context, settings);

    /// This is needed otherwise using an external stream with ProtobufSingle format as the target stream
    /// of a MV (or in `INSERT ... SELECT ...`), i.e. more than one rows sent to the stream, exception will be thrown.
    format_settings.protobuf.allow_multiple_rows_without_delimiter = true;

    /// This is required otherwise tools like Spark can't read data of string columns.
    format_settings.parquet.output_string_as_string = true;
    return format_settings;
}

}

}
#endif
