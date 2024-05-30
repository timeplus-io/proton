#include <Common/parseRemoteDescription.h>
#include <Storages/Distributed/DistributedSettings.h>
#include <Storages/StorageDistributed.h>
#include <Storages/ExternalStream/Proton/Proton.h>

namespace DB
{

namespace ExternalStream
{

Proton::Proton(IStorage * storage, std::unique_ptr<ExternalStreamSettings> settings_, ContextPtr context_)
: StorageExternalStreamImpl(storage, std::move(settings_), context_)
, logger(&Poco::Logger::get("ProtonExternalStream"))
{
    String cluster_description; /// FIXME
    std::vector<String> shards = parseRemoteDescription(cluster_description, 0, cluster_description.size(), ',', /*max_addresses=*/ 10);

    std::vector<std::vector<String>> names;
    names.reserve(shards.size());
    for (const auto & shard : shards)
        names.push_back(parseRemoteDescription(shard, 0, shard.size(), '|', /*max_addresses=*/ 10));

    auto maybe_secure_port = context->getTCPPortSecure();

    bool treat_local_as_remote = false;
    bool treat_local_port_as_remote = context->getApplicationType() == Context::ApplicationType::LOCAL;

    cluster = std::make_shared<Cluster>(
        context->getSettings(),
        names,
        /*username=*/ "default", // FIXME
        /*password=*/ "", /// FIXME
        (secure ? (maybe_secure_port ? *maybe_secure_port : DBMS_DEFAULT_SECURE_PORT) : context->getTCPPort()),
        treat_local_as_remote,
        treat_local_port_as_remote,
        secure);

    remote_stream_id.database_name = "default"; /// FIXME
    remote_stream_id.table_name = "foo"; /// FIXME

    /// StorageDistributed supports mismatching structure of remote table, so we can use outdated structure for CREATE ... AS remote(...)
    /// without additional conversion in StorageTableFunctionProxy
    cached_columns = getStructureOfRemoteTable(*cluster, remote_stream_id, context, /*table_func_ptr=*/ nullptr);

    storage_ptr = StorageDistributed::create(
        storage_id,
        cached_columns,
        ConstraintsDescription{},
        String{},
        remote_stream_id.database_name,
        remote_stream_id.table_name,
        String{},
        context,
        /*sharding_key=*/ nullptr, /// FIXME
        String{},
        String{},
        DistributedSettings{},
        false,
        cluster);
}

void Proton::startup()
{
    LOG_INFO(logger, "Starting");
    storage_ptr->startup();
}

void Proton::shutdown()
{
    LOG_INFO(logger, "Shutting down");
    storage_ptr->shutdown();
}

Pipe Proton::read(
    const Names &  /*column_names*/,
    const StorageSnapshotPtr &  /*storage_snapshot*/,
    SelectQueryInfo &  /*query_info*/,
    ContextPtr  /*context*/,
    QueryProcessingStage::Enum  /*processed_stage*/,
    size_t  /*max_block_size*/,
    size_t  /*num_streams*/)
{
    return {};
}

SinkToStoragePtr Proton::write(
    const ASTPtr &  /*query*/,
    const StorageMetadataPtr &  /*metadata_snapshot*/,
    ContextPtr  /*context*/)
{
    return nullptr;
}
}

}
