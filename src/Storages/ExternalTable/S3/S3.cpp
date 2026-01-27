#include <Storages/ExternalTable/S3/S3.h>

#if USE_AWS_S3

#include <Formats/FormatFactory.h>
#include <Storages/ExternalDataSourceConfiguration.h>
#include <Storages/ExternalTable/ExternalTableFactory.h>
#include <Storages/ExternalTable/S3/StorageS3.h>
#include <Storages/PartitionedSink.h>

namespace DB
{

namespace ErrorCodes
{
extern const int INVALID_SETTING_VALUE;
}

namespace ExternalTable
{

namespace
{
String inferSelectKeyFromInsertKey(const String & insert_key)
{
    /// Example:
    /// "{_partition_key}/data.json.gzip" will become "**/data.*.json.gzip"
    auto key = PartitionedSink::replaceWildcards(insert_key, "**");

    auto pos = key.find_first_of('.');
    return fmt::format("{}.{}{}", key.substr(0, pos), "*", (pos == std::string::npos ? "" : key.substr(pos)));
}
}

S3::S3(
    const StorageID & storage_id,
    const StorageInMemoryMetadata & storage_metadata,
    std::unique_ptr<ExternalTableSettings> external_table_settings,
    bool attach,
    ContextPtr context_)
    : StorageExternalTable(storage_id, storage_metadata, std::move(external_table_settings), attach, context_)
{
    StorageS3::Configuration configuration;
    /// Client configs are customizable via the query SETTINGS, for example, to use customize retries, timeouts, throttling, etc.
    configuration.static_configuration = false;

    auto bucket = settings->bucket.value;

    auto endpoint = settings->endpoint.value;
    if (endpoint.empty())
    {
        if (bucket.empty())
            throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "bucket cannot be empty");

        auto region = settings->region.value;
        if (region.empty())
            throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "region cannot be empty when endpoint is not provided");

        /// Until AWS support using bucket names that contain dots with virtual-hosted–style URL,
        /// we need to use path-styel URL for the best compatibility.
        configuration.url = DB::S3::URI(fmt::format("https://s3.{}.amazonaws.com/{}/", region, bucket));
    }
    else
    {
        if (!endpoint.ends_with("/"))
            endpoint += "/";
        if (!bucket.empty())
            endpoint = endpoint + bucket + "/";

        configuration.url = DB::S3::URI(endpoint);
    }


    auto select_key = settings->read_from.value;
    auto insert_key = settings->write_to.value;

    if (select_key.empty() && insert_key.empty())
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "select_key and insert_key cannot be empty at the same time");

    if (settings->data_format.changed)
        configuration.format = settings->data_format;

    if (configuration.format == "auto")
    {
        /// To detect data format from select_key and insert_key.
        /// It's fine not to be able to detect from one of the keys. For example, the select_key could be '*' (means everything).
        /// But if data formats are detected from both keys, they must be the same.
        String select_format;
        try
        {
            if (!select_key.empty())
                select_format = FormatFactory::instance().getFormatFromFileName(select_key, true);
        }
        catch (const Exception &)
        {
        }

        String insert_format;
        try
        {
            if (!insert_key.empty())
                insert_format = FormatFactory::instance().getFormatFromFileName(insert_key, true);
        }
        catch (const Exception &)
        {
        }

        if (select_format.empty() && insert_format.empty())
            throw Exception(
                ErrorCodes::INVALID_SETTING_VALUE,
                "Cannot detect data format from select_key ({}) nor insert_key ({})",
                select_format,
                insert_format);

        if (!select_format.empty() && !insert_format.empty() && select_format != insert_format)
            throw Exception(
                ErrorCodes::INVALID_SETTING_VALUE,
                "The data formats detected from select_key ({}) and insert_key ({}) do not match",
                select_format,
                insert_format);

        if (!select_format.empty())
            configuration.format = select_format;

        if (!insert_format.empty())
            configuration.format = insert_format;

        /// The JSON format is decided for write-only. In our case, if the users use ".json" file extension,
        /// we should use JSONEachRow for the default format. Othewise, people will be confused that they will
        /// get error "Format JSON is not suitable for input (with processors)" when they execute a SELECT query.
        if (configuration.format == "JSON")
            configuration.format = "JSONEachRow";
    }

    const auto & ctx_settings = context_->getSettingsRef();

    configuration.min_upload_file_size = ctx_settings.s3_min_upload_file_size;
    configuration.max_upload_idle_seconds = ctx_settings.s3_max_upload_idle_seconds;

    configuration.compression_method = settings->compression_method;
    configuration.auth_settings.access_key_id = settings->access_key_id;
    configuration.auth_settings.secret_access_key = settings->secret_access_key;
    configuration.auth_settings.region = settings->region;
    configuration.auth_settings.use_environment_credentials = settings->use_environment_credentials;

    configuration.request_settings.updateFromSettings(ctx_settings);

    FormatFactorySettings user_format_settings;

    // Firstly, apply changed settings from global context.
    for (const auto & change : ctx_settings.changes())
    {
        if (user_format_settings.has(change.name))
            user_format_settings.set(change.name, change.value);
    }

    // Then apply changes from SETTINGS clause, with validation.
    for (const auto & change : settings->changes())
    {
        if (user_format_settings.has(change.name))
            user_format_settings.set(change.name, change.value);
    }

    if (select_key.empty())
    {
        select_key = inferSelectKeyFromInsertKey(insert_key);
        LOG_INFO(
            getLogger(fmt::format("S3ExternalStream({})", getStorageID().table_name)),
            "Inferred select_key '{}' from insert_key '{}'",
            select_key,
            insert_key);
    }

    storage_ptr = StorageS3::create(
        configuration,
        getStorageID(),
        storage_metadata.columns,
        storage_metadata.constraints,
        storage_metadata.comment,
        getContext(),
        std::move(user_format_settings),
        select_key,
        insert_key,
        attach,
        /*distributed_processing_=*/false,
        storage_metadata.isPartitionKeyDefined() ? storage_metadata.getPartitionKey().definition_ast : nullptr);
}

void S3::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    setThreadName("ExtTblReader");
    storage_ptr->read(query_plan, column_names, storage_snapshot, query_info, local_context, processed_stage, max_block_size, num_streams);
}

SinkToStoragePtr S3::writeImpl(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context)
{
    if (query_context->isQueryFromMaterializedView())
    {
        /// When writing to S3 from a MV, by default, it retris as much as it can, so that it can
        /// resume sending data when the error is gone to avoid losing data.
        auto new_context = Context::createCopy(query_context);

        if (!query_context->getSettingsRef().isChanged("s3_max_aws_request_failure_retries"))
            new_context->setSetting("s3_max_aws_request_failure_retries", std::numeric_limits<Int64>::max());

        return storage_ptr->write(query, metadata_snapshot, new_context);
    }
    else
    {
        return storage_ptr->write(query, metadata_snapshot, query_context);
    }
}

void S3::truncate(
    const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context, TableExclusiveLockHolder & lock_holder)
{
    storage_ptr->truncate(query, metadata_snapshot, local_context, lock_holder);
}

}

void registerS3ExternalTable(ExternalTableFactory & factory)
{
    factory.registerExternalTable(
        "s3",
        [](const StorageID & table_id,
           const StorageInMemoryMetadata & storage_metadata,
           const ASTCreateQuery &,
           std::unique_ptr<ExternalTableSettings> settings,
           bool attach,
           ContextPtr context) -> StoragePtr {
            return std::make_unique<ExternalTable::S3>(table_id, storage_metadata, std::move(settings), attach, std::move(context));
        });
}

}

#else

namespace DB
{

class ExternalTableFactory;

void registerS3ExternalTable(ExternalTableFactory &)
{
}

}

#endif
