#include <Storages/ExternalStream/Timeplus/Timeplus.h>

#include "config_version.h"

#include <DataTypes/DataTypeFactory.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/getHeaderForProcessingStage.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Storages/Distributed/DistributedSettings.h>
#include <Storages/StorageDistributed.h>
#include <Storages/getStreamShardsOfRemoteStream.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/parseRemoteDescription.h>

namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_CREATE_DIRECTORY;
extern const int INCOMPATIBLE_COLUMNS;
extern const int INVALID_SETTING_VALUE;
extern const int QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW;
}

namespace
{

StorageID getRemoteStreamStorageID(const ExternalStreamSettings & settings)
{
    if (settings.stream.value.empty())
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Setting `stream` cannot be empty.");
    return {settings.db.value, settings.stream.value};
}

void createDirIfNotExists(const String & dirname)
{
    std::error_code err;
    /// create_directories will do nothing if the directory already exists.
    fs::create_directories(dirname, err);
    if (err)
        throw Exception(
            ErrorCodes::CANNOT_CREATE_DIRECTORY,
            "Failed to create directory for storing remote stream metadata, error_code={}, error_mesage={}",
            err.value(),
            err.message());
}

void tryRemoveDir(const fs::path & dirname, Poco::Logger * logger)
{
    LOG_INFO(logger, "Trying to remove remote stream metadata directory {}", dirname.string());
    std::error_code err;
    fs::remove_all(dirname, err);
    if (err)
        LOG_ERROR(
            logger, "Failed to remove the remote stream metadata directory, error_code={}, error_message={}", err.value(), err.message());
}

void cacheStreamMetadata(const fs::path & filename, const ColumnsDescription & columns, UInt32 shard_count)
{
    WriteBufferFromFile wbuf{filename};
    writeVarUInt(shard_count, wbuf);
    auto columns_str = columns.toString();
    writeString(columns_str.data(), columns_str.size(), wbuf);
}

void loadCachedStreamMetadata(const fs::path & filename, ColumnsDescription & columns, UInt32 & shard_count)
{
    ReadBufferFromFile rbuf{filename};

    readVarUInt(shard_count, rbuf);

    String columns_str;
    readStringUntilEOF(columns_str, rbuf);

    columns = ColumnsDescription::parse(columns_str);
}

}

namespace ExternalStream
{

Timeplus::Timeplus(
    IStorage * storage,
    StorageInMemoryMetadata & storage_metadata,
    std::unique_ptr<ExternalStreamSettings> settings_,
    bool attach,
    ContextPtr context)
    : StorageProxy(storage->getStorageID())
    , remote_stream_id(getRemoteStreamStorageID(*settings_))
    , cache_dir(
          fs::path(context->getConfigRef().getString("path", DBMS_DEFAULT_PATH)) / "cache" / "external_streams"
          / toString(getStorageID().uuid))
    , secure(settings_->secure)
    , logger(&Poco::Logger::get(getName()))
{
    createDirIfNotExists(cache_dir);

    const auto & hosts = settings_->hosts.value;
    if (hosts.empty())
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Setting `hosts` cannot be empty.");

    auto default_port = secure ? DBMS_DEFAULT_SECURE_PORT : DBMS_DEFAULT_PORT;
    auto addresses = parseRemoteDescriptionForExternalDatabase(hosts, /*max_addresses=*/10, /*default_port=*/default_port);

    std::vector<std::vector<String>> names;
    names.reserve(addresses.size());
    for (const auto & addr : addresses)
        names.push_back({fmt::format("{}:{}", addr.first, addr.second)});

    auto user = settings_->user.value;
    Cluster init_cluster
        = {context->getSettings(),
           names,
           /*username=*/user.empty() ? "default" : user,
           /*password=*/settings_->password.value,
           context->getTCPPort(),
           /*treat_local_as_remote=*/true, /// always treat the connection as remote to avoid confusion
           /*treat_local_port_as_remote*/ true,
           secure};

    ColumnsDescription columns;
    UInt32 remote_stream_shards{0};

    auto cache_file = cache_dir / "metadata";
    bool metadata_loaded{false};

    if (attach)
    {
        if (!fs::exists(cache_file))
        {
            LOG_WARNING(logger, "Meatadata cache file {} does not exist, will try fetch from remote.", cache_file.string());
        }
        else
        {
            try
            {
                loadCachedStreamMetadata(cache_file, columns, remote_stream_shards);
                LOG_INFO(
                    logger, "Loaded remote stream metadata from cache, columns_size={} shards={}", columns.size(), remote_stream_shards);
                metadata_loaded = true;
            }
            catch (...)
            {
                LOG_WARNING(
                    logger,
                    "Failed to load cached remote stream metadata, will try fetch from remote, error: {}",
                    getCurrentExceptionMessage(/*with_stacktrace=*/false));
            }
        }
    }

    if (!metadata_loaded)
    {
        bool encountered_error{false};
        try
        {
            /// StorageDistributed supports mismatching structure of remote table, so we can use outdated structure for CREATE ... AS remote(...)
            /// without additional conversion in StorageTableFunctionProxy
            columns = getStructureOfRemoteTable(init_cluster, remote_stream_id, context, /*table_func_ptr=*/nullptr);
        }
        catch (const Exception & e)
        {
            if (!attach)
                e.rethrow();

            encountered_error = true;

            /// When attach, like proton restarts, it should not throw any exception,
            /// otherwise proton will crash. It can't keep retrying neither, or it will
            /// block proton becoming ready.
            /// TODO: https://github.com/timeplus-io/proton-enterprise/issues/6762
            auto error_msg = fmt::format("Failed to get structure of remote stream: {}", e.message());
            LOG_ERROR(logger, "{}", error_msg);

            /// A stream can't have no columns, thus add a dummy column to show the error as comment.
            auto col = ColumnDescription("_error", DataTypeFactory::instance().get(TypeIndex::String));
            col.comment = error_msg;
            columns.add(col);
        }

        try
        {
            remote_stream_shards = getStreamShardsOfRemoteStream(init_cluster, remote_stream_id, context);
        }
        catch (const Exception & e)
        {
            if (!attach)
                e.rethrow();

            /// See the comment above for fetching columns.
            encountered_error = true;
            LOG_ERROR(logger, "Failed to get the number of shards of remote stream: {}", e.message());
        }

        if (!encountered_error)
            cacheStreamMetadata(cache_file, columns, remote_stream_shards);
    }

    storage_metadata.setColumns(columns);

    for (UInt32 i = 1; i < remote_stream_shards; ++i)
        names.push_back(names.front());

    auto cluster = std::make_shared<Cluster>(
        context->getSettings(),
        names,
        /*username=*/user.empty() ? "default" : user,
        /*password=*/settings_->password.value,
        static_cast<UInt16>(default_port),
        /*treat_local_as_remote=*/true, /// always treat the connection as remote to avoid confusion
        /*treat_local_port_as_remote*/ true,
        secure);

    storage_ptr = StorageDistributed::create(
        storage->getStorageID(),
        columns,
        ConstraintsDescription{},
        /*comment=*/String{},
        remote_stream_id.database_name,
        remote_stream_id.table_name,
        /*cluster_name_=*/String{},
        context,
        /*sharding_key=*/nullptr, /// TODO: issues#782
        /*storage_policy_name_=*/String{},
        /*relative_data_path_=*/String{},
        DistributedSettings{},
        /*attach_=*/false,
        cluster);
}

void Timeplus::drop()
{
    tryRemoveDir(cache_dir, logger);
    storage_ptr->drop();
}

void Timeplus::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & /*storage_snapshot*/,
    SelectQueryInfo & query_info,
    ContextPtr context_,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    auto new_context = Context::createCopy(context_);
    /// client_name is required (and it MUST start with VERSION_NAME) to enable internal channel on the remote server to set the SN on blocks.
    /// When timeplusd restarts, and a stream is used in a MV, when the MV recovers, client_name will be empty.
    if (new_context->getClientInfo().client_name.empty())
        new_context->getClientInfo().client_name = VERSION_NAME;

    auto nested_snapshot = storage_ptr->getStorageSnapshot(storage_ptr->getInMemoryMetadataPtr(), new_context);
    storage_ptr->read(query_plan, column_names, nested_snapshot, query_info, new_context, processed_stage, max_block_size, num_streams);
}

SinkToStoragePtr Timeplus::write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context_)
{
    auto cached_structure = metadata_snapshot->getSampleBlock();
    auto actual_structure = storage_ptr->getInMemoryMetadataPtr()->getSampleBlock();
    if (!blocksHaveEqualStructure(actual_structure, cached_structure))
        throw Exception(ErrorCodes::INCOMPATIBLE_COLUMNS, "Inconsistent storage structure");

    auto new_context = Context::createCopy(context_);
    /// All shards of a stream are the same, just write to any one.
    new_context->setSetting("insert_distributed_one_random_shard", true);
    return storage_ptr->write(query, metadata_snapshot, new_context);
}

}

}
