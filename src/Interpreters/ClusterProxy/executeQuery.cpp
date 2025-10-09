#include <Core/QueryProcessingStage.h>
#include <Core/Settings.h>
#include <Core/UUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/ObjectUtils.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/OptimizeShardingKeyRewriteInVisitor.h>
#include <Interpreters/ProcessList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/queryToString.h>
#include <Processors/QueryPlan/DistributedCreateLocalPlan.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromRemote.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/ResizeProcessor.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/Distributed/DistributedSettings.h>
#include <Storages/SelectQueryInfo.h>

#include <ranges>

namespace DB
{

namespace ErrorCodes
{
extern const int TOO_LARGE_DISTRIBUTED_DEPTH;
extern const int LOGICAL_ERROR;
extern const int SUPPORT_IS_DISABLED;
}

namespace ClusterProxy
{

ContextMutablePtr updateSettingsAndClientInfoForCluster(
    const Cluster & cluster,
    bool is_remote_function,
    ContextPtr context,
    const Settings & settings,
    const StorageID & main_table,
    ASTPtr additional_filter_ast,
    LoggerPtr log,
    const DistributedSettings * distributed_settings)
{
    ClientInfo new_client_info = context->getClientInfo();
    Settings new_settings{settings};
    new_settings.queue_max_wait_ms = Cluster::saturate(new_settings.queue_max_wait_ms, settings.max_execution_time);

    /// In case of interserver mode we should reset initial_user for remote() function to use passed user from the query.
    if (is_remote_function)
    {
        const auto & address = cluster.getShardsAddresses().front().front();
        new_client_info.initial_user = address.user;
    }

    /// If "secret" (in remote_servers) is not in use,
    /// user on the shard is not the same as the user on the initiator,
    /// hence per-user limits should not be applied.
    const bool interserver_mode = !cluster.getSecret().empty();
    if (!interserver_mode)
    {
        /// Does not matter on remote servers, because queries are sent under different user.
        new_settings.max_concurrent_queries_for_user = 0;
        new_settings.max_memory_usage_for_user = 0;

        /// Set as unchanged to avoid sending to remote server.
        new_settings.max_concurrent_queries_for_user.changed = false;
        new_settings.max_memory_usage_for_user.changed = false;
    }

    if (settings.force_optimize_skip_unused_shards_nesting && settings.force_optimize_skip_unused_shards)
    {
        if (new_settings.force_optimize_skip_unused_shards_nesting == 1)
        {
            new_settings.force_optimize_skip_unused_shards = false;
            new_settings.force_optimize_skip_unused_shards.changed = false;

            if (log)
                LOG_TRACE(
                    log,
                    "Disabling force_optimize_skip_unused_shards for nested queries (force_optimize_skip_unused_shards_nesting exceeded)");
        }
        else
        {
            --new_settings.force_optimize_skip_unused_shards_nesting.value;
            new_settings.force_optimize_skip_unused_shards_nesting.changed = true;

            if (log)
                LOG_TRACE(
                    log, "force_optimize_skip_unused_shards_nesting is now {}", new_settings.force_optimize_skip_unused_shards_nesting);
        }
    }

    if (settings.optimize_skip_unused_shards_nesting && settings.optimize_skip_unused_shards)
    {
        if (new_settings.optimize_skip_unused_shards_nesting == 1)
        {
            new_settings.optimize_skip_unused_shards = false;
            new_settings.optimize_skip_unused_shards.changed = false;

            if (log)
                LOG_TRACE(log, "Disabling optimize_skip_unused_shards for nested queries (optimize_skip_unused_shards_nesting exceeded)");
        }
        else
        {
            --new_settings.optimize_skip_unused_shards_nesting.value;
            new_settings.optimize_skip_unused_shards_nesting.changed = true;

            if (log)
                LOG_TRACE(log, "optimize_skip_unused_shards_nesting is now {}", new_settings.optimize_skip_unused_shards_nesting);
        }
    }

    if (!settings.skip_unavailable_shards.changed && distributed_settings)
    {
        new_settings.skip_unavailable_shards = distributed_settings->skip_unavailable_shards.value;
        new_settings.skip_unavailable_shards.changed = true;
    }

    if (settings.offset)
    {
        new_settings.offset = 0;
        new_settings.offset.changed = false;
    }
    if (settings.limit)
    {
        new_settings.limit = 0;
        new_settings.limit.changed = false;
    }

    /// Setting additional_table_filters may be applied to Distributed table.
    /// In case if query is executed up to WithMergableState on remote shard, it is impossible to filter on initiator.
    /// We need to propagate the setting, but change the table name from distributed to source.
    ///
    /// Here we don't try to analyze setting again. In case if query_info->additional_filter_ast is not empty, some filter was applied.
    /// It's just easier to add this filter for a source table.
    if (additional_filter_ast)
    {
        Tuple tuple;
        tuple.push_back(main_table.getShortName());
        tuple.push_back(queryToString(additional_filter_ast, true));
        new_settings.additional_table_filters.value.push_back(std::move(tuple));
    }

    /// disable parallel replicas if cluster contains only shards with 1 replica
    if (context->canUseTaskBasedParallelReplicas())
    {
        bool disable_parallel_replicas = true;
        for (const auto & shard : cluster.getShardsInfo())
        {
            if (shard.getAllNodeCount() > 1)
            {
                disable_parallel_replicas = false;
                break;
            }
        }
        if (disable_parallel_replicas)
            new_settings.allow_experimental_parallel_reading_from_replicas = 0;
    }

    if (settings.max_execution_time_leaf.value > 0)
    {
        /// Replace 'max_execution_time' of this sub-query with 'max_execution_time_leaf' and 'timeout_overflow_mode'
        /// with 'timeout_overflow_mode_leaf'
        new_settings.max_execution_time = settings.max_execution_time_leaf;
        new_settings.timeout_overflow_mode = settings.timeout_overflow_mode_leaf;
    }

    /// in case of parallel replicas custom key use round robing load balancing
    /// so custom key partitions will be spread over nodes in round-robin fashion
    if (context->canUseParallelReplicasCustomKeyForCluster(cluster) && !settings.load_balancing.changed)
    {
        new_settings.load_balancing = LoadBalancing::ROUND_ROBIN;
    }

    auto new_context = Context::createCopy(context);
    new_context->setSettings(new_settings);
    new_context->setClientInfo(new_client_info);

    if (context->canUseParallelReplicasCustomKeyForCluster(cluster))
        new_context->disableOffsetParallelReplicas();

    return new_context;
}

ContextMutablePtr
updateSettingsForCluster(const Cluster & cluster, ContextPtr context, const Settings & settings, const StorageID & main_table)
{
    return updateSettingsAndClientInfoForCluster(
        cluster,
        /*is_remote_function=*/false,
        context,
        settings,
        main_table,
        /*additional_filter_ast=*/{},
        /*log=*/{},
        /*distributed_settings=*/{});
}

static ThrottlerPtr getThrottler(const ContextPtr & context)
{
    const Settings & settings = context->getSettingsRef();

    ThrottlerPtr user_level_throttler;
    if (auto process_list_element = context->getProcessListElement())
        user_level_throttler = process_list_element->getUserNetworkThrottler();

    /// Network bandwidth limit, if needed.
    ThrottlerPtr throttler;
    if (settings.max_network_bandwidth || settings.max_network_bytes)
    {
        throttler = std::make_shared<Throttler>(
            settings.max_network_bandwidth,
            settings.max_network_bytes,
            "Limit for bytes to send or receive over network exceeded.",
            user_level_throttler);
    }
    else
        throttler = user_level_throttler;

    return throttler;
}

void executeQuery(
    QueryPlan & query_plan,
    const Block & header,
    QueryProcessingStage::Enum processed_stage,
    const StorageID & main_table,
    const ASTPtr & table_func_ptr,
    SelectStreamFactory & stream_factory,
    LoggerPtr log,
    ContextPtr context,
    const SelectQueryInfo & query_info,
    const ExpressionActionsPtr & sharding_key_expr,
    const std::string & sharding_key_column_name,
    const DistributedSettings & distributed_settings,
    AdditionalShardFilterGenerator shard_filter_generator,
    bool is_remote_function,
    /// proton: starts.
    bool is_streaming,
    bool read_concat,
    std::optional<UInt32> shard_to_read)
/// proton: ends.
{
    assert(log);

    const Settings & settings = context->getSettingsRef();

    if (settings.max_distributed_depth && context->getClientInfo().distributed_depth > settings.max_distributed_depth)
        throw Exception(ErrorCodes::TOO_LARGE_DISTRIBUTED_DEPTH, "Maximum distributed depth exceeded");

    const auto & not_optimized_cluster = query_info.cluster;

    std::vector<QueryPlanPtr> plans;
    SelectStreamFactory::Shards remote_shards;

    auto cluster = query_info.getCluster();
    auto new_context = updateSettingsAndClientInfoForCluster(
        *cluster, is_remote_function, context, settings, main_table, query_info.additional_filter_ast, log, &distributed_settings);

    new_context->increaseDistributedDepth();

    /// proton: starts.
    new_context->setSetting("query_mode", is_streaming ? String("streaming") : String("table"));
    new_context->resetSettingsToDefaultValue({"exec_mode"}); /// Reset exec_mode to ExecuteMode::Normal.
    if (const auto & node_ids = query_info.getCluster()->nodes(); !node_ids.empty())
    {
        auto nodes_view = node_ids | std::views::transform([](auto node_id) { return node_id; });
        new_context->setSetting("target_nodes", fmt::format("{}", fmt::join(nodes_view, ",")));
    }

    if (query_info.seek_to_info)
        new_context->applySettingChange({"seek_to", query_info.seek_to_info->getSeekToForSettings()});
    /// proton: ends.

    const size_t shards = cluster->getShardCount();
    for (size_t i = 0, s = cluster->getShardsInfo().size(); i < s; ++i)
    {
        const auto & shard_info = cluster->getShardsInfo()[i];

        /// proton: starts.
        if (shard_to_read.has_value() && shard_to_read.value() != (shard_info.shard_num - 1))
            continue;
        /// proton: ends.

        ASTPtr query_ast_for_shard = query_info.query->clone();
        if (sharding_key_expr && query_info.optimized_cluster && settings.optimize_skip_unused_shards_rewrite_in && shards > 1 &&
            /// TODO: support composite sharding key
            sharding_key_expr->getRequiredColumns().size() == 1)
        {
            OptimizeShardingKeyRewriteInVisitor::Data visitor_data{
                sharding_key_expr,
                sharding_key_expr->getSampleBlock().getByPosition(0).type,
                sharding_key_column_name,
                shard_info,
                not_optimized_cluster->getSlotToShard(),
            };
            OptimizeShardingKeyRewriteInVisitor visitor(visitor_data);
            visitor.visit(query_ast_for_shard);
        }

        if (shard_filter_generator)
        {
            auto shard_filter = shard_filter_generator(shard_info.shard_num);
            if (shard_filter)
            {
                auto & select_query = query_ast_for_shard->as<ASTSelectQuery &>();

                auto where_expression = select_query.where();
                if (where_expression)
                    shard_filter = makeASTFunction("and", where_expression, shard_filter);

                select_query.setExpression(ASTSelectQuery::Expression::WHERE, std::move(shard_filter));
            }
        }

        // decide for each shard if parallel reading from replicas should be enabled
        // according to settings and number of replicas declared per shard
        const auto & addresses = cluster->getShardsAddresses().at(i);
        bool parallel_replicas_enabled = addresses.size() > 1 && context->canUseTaskBasedParallelReplicas();

        stream_factory.createForShard(
            shard_info,
            query_ast_for_shard,
            main_table,
            table_func_ptr,
            new_context,
            plans,
            remote_shards,
            static_cast<UInt32>(shards),
            parallel_replicas_enabled,
            shard_filter_generator);
    }

    if (!remote_shards.empty())
    {
        Scalars scalars = context->hasQueryContext() ? context->getQueryContext()->getScalars() : Scalars{};
        scalars.emplace(
            "_shard_count", Block{{DataTypeUInt32().createColumnConst(1, shards), std::make_shared<DataTypeUInt32>(), "_shard_count"}});
        auto external_tables = context->getExternalTables();

        auto plan = std::make_unique<QueryPlan>();
        auto read_from_remote = std::make_unique<ReadFromRemote>(
            std::move(remote_shards),
            header,
            processed_stage,
            main_table,
            table_func_ptr,
            new_context,
            getThrottler(context),
            std::move(scalars),
            std::move(external_tables),
            log,
            shards,
            query_info.storage_limits,
            read_concat);

        /// proton: starts.
        if (shard_to_read.has_value())
            read_from_remote->setStepDescription(fmt::format("Read from remote replica of shard-{}", *shard_to_read));
        else
            read_from_remote->setStepDescription("Read from remote replica");
        /// proton: ends.

        plan->addStep(std::move(read_from_remote));
        plan->addInterpreterContext(new_context);
        plans.emplace_back(std::move(plan));
    }

    if (plans.empty())
        return;

    if (plans.size() == 1)
    {
        query_plan = std::move(*plans.front());
        return;
    }

    DataStreams input_streams;
    input_streams.reserve(plans.size());
    for (auto & plan : plans)
        input_streams.emplace_back(plan->getCurrentDataStream());

    auto union_step = std::make_unique<UnionStep>(std::move(input_streams));
    query_plan.unitePlans(std::move(union_step), std::move(plans));
}

void executeQueryWithParallelReplicas(
    QueryPlan & query_plan,
    const StorageID & main_table,
    const ASTPtr & table_func_ptr,
    SelectStreamFactory & stream_factory,
    const ASTPtr & query_ast,
    ContextPtr context,
    const SelectQueryInfo & query_info,
    const ClusterPtr & not_optimized_cluster,
    bool is_streaming,
    bool parallelized_to_all_replicas)
{
    if (not_optimized_cluster->getShardsInfo().size() != 1)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Cluster for parallel replicas should consist only from one shard");

    auto shard_info = not_optimized_cluster->getShardsInfo().front();

    const auto & settings = context->getSettingsRef();
    size_t all_replicas_count = shard_info.all_addresses.size();
    if (!parallelized_to_all_replicas)
        all_replicas_count = std::min(static_cast<size_t>(settings.max_parallel_replicas), all_replicas_count);

    auto coordinator = std::make_shared<ParallelReplicasReadingCoordinator>(all_replicas_count);
    auto remote_plan = std::make_unique<QueryPlan>();
    auto plans = std::vector<QueryPlanPtr>();

    /// This is a little bit weird, but we construct an "empty" coordinator without
    /// any specified reading/coordination method (like Default, InOrder, InReverseOrder)
    /// Because we will understand it later during QueryPlan optimization
    /// So we place a reference to the coordinator to some common plane like QueryInfo
    /// to then tell it about the reading method we chose.
    query_info.coordinator = coordinator;

    UUID parallel_group_id = UUIDHelpers::generateV4();

    if (shard_info.hasLocalConnections())
        plans.emplace_back(createLocalPlan(
            query_ast,
            stream_factory.header,
            context,
            stream_factory.processed_stage,
            shard_info.shard_num,
            /*shard_count=*/1,
            /*has_missing_objects=*/false,
            is_streaming));

    if (!shard_info.hasRemoteConnections())
    {
        if (plans.empty() || !plans.front())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "An empty plan was generated to read from local shard and there is no remote connections. This is a bug");

        query_plan = std::move(*plans.front());
        return;
    }

    auto new_context = Context::createCopy(context);
    auto scalars = new_context->hasQueryContext() ? new_context->getQueryContext()->getScalars() : Scalars{};
    auto external_tables = new_context->getExternalTables();

    new_context->setSetting("max_parallel_replicas", all_replicas_count);
    new_context->setSetting("query_mode", is_streaming ? String{"streaming"} : String{"table"});

    auto read_from_remote = std::make_unique<ReadFromParallelRemoteReplicasStep>(
        query_ast,
        std::move(shard_info),
        std::move(coordinator),
        stream_factory.header,
        stream_factory.processed_stage,
        main_table,
        table_func_ptr,
        new_context,
        getThrottler(new_context),
        std::move(scalars),
        std::move(external_tables),
        getLogger("ReadFromParallelRemoteReplicasStep"),
        static_cast<UInt32>(query_info.getCluster()->getShardCount()),
        parallel_group_id);

    remote_plan->addStep(std::move(read_from_remote));
    remote_plan->addInterpreterContext(context);
    plans.emplace_back(std::move(remote_plan));

    if (std::all_of(plans.begin(), plans.end(), [](const QueryPlanPtr & plan) { return !plan; }))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No plans were generated for reading from shard. This is a bug");

    DataStreams input_streams;
    input_streams.reserve(plans.size());
    for (const auto & plan : plans)
        input_streams.emplace_back(plan->getCurrentDataStream());

    auto union_step = std::make_unique<UnionStep>(std::move(input_streams));
    query_plan.unitePlans(std::move(union_step), std::move(plans));
}

}
}
