#include <Common/parseRemoteDescription.h>
#include <Interpreters/getHeaderForProcessingStage.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Storages/Distributed/DistributedSettings.h>
#include <Storages/StorageDistributed.h>
#include <Storages/ExternalStream/Proton/Proton.h>

namespace DB
{

namespace ErrorCodes
{
extern const int INCOMPATIBLE_COLUMNS;
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

Proton::Proton(IStorage * storage, StorageInMemoryMetadata & storage_metadata, std::unique_ptr<ExternalStreamSettings> settings_, bool attach, ContextPtr context)
: StorageProxy(storage->getStorageID())
, remote_stream_id(getRemoteStreamStorageID(storage->getStorageID(), *settings_))
, logger(&Poco::Logger::get(getName()))
{
    LOG_INFO(logger, "attach = {}", attach);

    String hosts = settings_->hosts.value;
    if (hosts.empty())
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Setting `hosts` cannot be empty.");

    std::vector<String> shards = parseRemoteDescription(hosts, 0, hosts.size(), ',', /*max_addresses=*/ 10);

    std::vector<std::vector<String>> names;
    names.reserve(shards.size());
    for (const auto & shard : shards)
        names.push_back(parseRemoteDescription(shard, 0, shard.size(), '|', /*max_addresses=*/ 10));

    auto maybe_secure_port = context->getTCPPortSecure();

    auto secure = settings_->secure;
    /// FIXME
    bool treat_local_as_remote = false;
    bool treat_local_port_as_remote = context->getApplicationType() == Context::ApplicationType::LOCAL;

    auto user = settings_->user.value;
    auto cluster = std::make_shared<Cluster>(
        context->getSettings(),
        names,
        /*username=*/ user.empty() ? "default" : user,
        /*password=*/ settings_->password.value,
        (secure ? (maybe_secure_port ? *maybe_secure_port : DBMS_DEFAULT_SECURE_PORT) : context->getTCPPort()),
        /*true,*/ treat_local_as_remote,
        /*true,*/ treat_local_port_as_remote,
        secure);

    /// StorageDistributed supports mismatching structure of remote table, so we can use outdated structure for CREATE ... AS remote(...)
    /// without additional conversion in StorageTableFunctionProxy
    auto columns = getStructureOfRemoteTable(*cluster, remote_stream_id, context, /*table_func_ptr=*/ nullptr);
    storage_metadata.setColumns(columns);

    storage_ptr = StorageDistributed::create(
        storage->getStorageID(),
        columns,
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
    String cnames;
    for (const auto & c : column_names)
        cnames += c + " ";
    auto nested_snapshot = storage_ptr->getStorageSnapshot(storage_ptr->getInMemoryMetadataPtr(), context_);
    storage_ptr->read(query_plan, column_names, nested_snapshot, query_info, context_,
                              processed_stage, max_block_size, num_streams);
    bool add_conversion{true}; /// TBD
    if (add_conversion)
    {
        auto from_header = query_plan.getCurrentDataStream().header;
        auto to_header = getHeaderForProcessingStage(column_names, storage_snapshot,
                                                     query_info, context_, processed_stage);

        auto convert_actions_dag = ActionsDAG::makeConvertingActions(
                from_header.getColumnsWithTypeAndName(),
                to_header.getColumnsWithTypeAndName(),
                ActionsDAG::MatchColumnsMode::Name);

        auto step = std::make_unique<ExpressionStep>(
            query_plan.getCurrentDataStream(),
            convert_actions_dag);

        step->setStepDescription("Converting columns");
        query_plan.addStep(std::move(step));
    }
}

SinkToStoragePtr Proton::write(
    const ASTPtr & query,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr context_)
{
    auto cached_structure = metadata_snapshot->getSampleBlock();
    auto actual_structure = storage_ptr->getInMemoryMetadataPtr()->getSampleBlock();
    auto add_conversion{true}; /// TBD
    if (!blocksHaveEqualStructure(actual_structure, cached_structure) && add_conversion)
    {
        throw Exception("Source storage and table function have different structure", ErrorCodes::INCOMPATIBLE_COLUMNS);
    }
    return storage_ptr->write(query, metadata_snapshot, context_);
}

}

}
