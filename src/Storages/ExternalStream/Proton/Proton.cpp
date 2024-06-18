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
, remote_stream_id(StorageID::createEmpty())
, logger(&Poco::Logger::get(getName()))
{
    LOG_INFO(logger, "Creating ...");
    String cluster_description = "127.0.0.1"; /// FIXME
    std::vector<String> shards = parseRemoteDescription(cluster_description, 0, cluster_description.size(), ',', /*max_addresses=*/ 10);

    LOG_INFO(logger, "shards' size = {}", shards.size());
    std::vector<std::vector<String>> names;
    names.reserve(shards.size());
    for (const auto & shard : shards)
        names.push_back(parseRemoteDescription(shard, 0, shard.size(), '|', /*max_addresses=*/ 10));

    auto maybe_secure_port = context->getTCPPortSecure();

    /// FIXME
    // bool treat_local_as_remote = false;
    // bool treat_local_port_as_remote = context->getApplicationType() == Context::ApplicationType::LOCAL;

    cluster = std::make_shared<Cluster>(
        context->getSettings(),
        names,
        /*username=*/ "default", // FIXME
        /*password=*/ "", /// FIXME
        (secure ? (maybe_secure_port ? *maybe_secure_port : DBMS_DEFAULT_SECURE_PORT) : context->getTCPPort()),
        true, /*treat_local_as_remote,*/
        true, /*treat_local_port_as_remote,*/
        secure);

    remote_stream_id.database_name = "default"; /// FIXME
    remote_stream_id.table_name = "foo"; /// FIXME

    /// StorageDistributed supports mismatching structure of remote table, so we can use outdated structure for CREATE ... AS remote(...)
    /// without additional conversion in StorageTableFunctionProxy
    cached_columns = getStructureOfRemoteTable(*cluster, remote_stream_id, context, /*table_func_ptr=*/ nullptr);

    storage_ptr = StorageDistributed::create(
        getStorageID(),
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

bool Proton::supportsSubcolumns() const
{
    return storage_ptr->supportsSubcolumns();
}

void Proton::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context_,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    storage_ptr->read(query_plan, column_names, storage_snapshot, query_info, context_, processed_stage, max_block_size, num_streams);
}

SinkToStoragePtr Proton::write(
    const ASTPtr & query,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr context_)
{
    return storage_ptr->write(query, metadata_snapshot, context_);
}
}

}
