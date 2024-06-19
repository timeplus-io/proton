#include <Common/parseRemoteDescription.h>
#include <Storages/Distributed/DistributedSettings.h>
#include <Storages/StorageDistributed.h>
#include <Storages/ExternalStream/Proton/Proton.h>

namespace DB
{

namespace ErrorCodes
{
extern const int INVALID_SETTING_VALUE;
}

namespace
{

StorageID getRemoteStreamStorageID(const StorageID & externalStreamStorageID, const ExternalStreamSettings & settings)
{
    auto db = settings.db.value;
    auto stream = settings.stream.value;
    return {
        db.empty() ? externalStreamStorageID.getDatabaseName() : db,
        stream.empty() ? externalStreamStorageID.getTableName() : stream
    };
}

}

namespace ExternalStream
{

Proton::Proton(IStorage * storage, std::unique_ptr<ExternalStreamSettings> settings_, ContextPtr context)
: StorageExternalStreamImpl(storage, std::move(settings_), context)
, remote_stream_id(getRemoteStreamStorageID(getStorageID(), *settings))
, logger(&Poco::Logger::get(getName()))
{
    String hosts = settings->hosts.value;
    if (hosts.empty())
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Setting `hosts` cannot be empty.");

    std::vector<String> shards = parseRemoteDescription(hosts, 0, hosts.size(), ',', /*max_addresses=*/ 10);

    std::vector<std::vector<String>> names;
    names.reserve(shards.size());
    for (const auto & shard : shards)
        names.push_back(parseRemoteDescription(shard, 0, shard.size(), '|', /*max_addresses=*/ 10));

    auto maybe_secure_port = context->getTCPPortSecure();

    /// FIXME
    bool treat_local_as_remote = false;
    bool treat_local_port_as_remote = context->getApplicationType() == Context::ApplicationType::LOCAL;

    auto user = settings->user.value;
    cluster = std::make_shared<Cluster>(
        context->getSettings(),
        names,
        /*username=*/ user.empty() ? "default" : user,
        /*password=*/ settings->password.value,
        (secure ? (maybe_secure_port ? *maybe_secure_port : DBMS_DEFAULT_SECURE_PORT) : context->getTCPPort()),
        /*true,*/ treat_local_as_remote,
        /*true,*/ treat_local_port_as_remote,
        secure);

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
