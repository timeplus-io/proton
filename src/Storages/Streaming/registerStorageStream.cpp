#include <Storages/Streaming/StorageStream.h>
#include <Storages/Streaming/StorageStreamProperties.h>
#include <Storages/StorageFactory.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

void registerStorageStream(StorageFactory & factory)
{
    StorageFactory::StorageFeatures features{
        .supports_settings = true,
        .supports_skipping_indices = true,
        .supports_projections = true,
        .supports_sort_order = true,
        .supports_ttl = true,
        .supports_replication = true,
        .supports_deduplication = true,
        .supports_parallel_insert = true,
        .supports_schema_inference = true};

    factory.registerStorage(
        "Stream",
        [](const StorageFactory::Arguments & args) {
            /** * Stream engine arguments : Stream(replication_factor, shards, shard_by_expr)
            * - replication_factor
            * - shards
            * - shard_by_expr

            * Stream engine settings :
            * - logstore=kafka
            * - logstore_cluster_id=<my_cluster>
            * - logstore_partition=<partition>
            * - logstore_request_required_acks=1
            * - logstore_request_timeout_ms=30000
            * - logstore_auto_offset_reset=earliest
            */

            MergeTreeData::MergingParams merging_params;
            merging_params.mode = MergeTreeData::MergingParams::Ordinary;
            /// NOTE Quite complicated.

            size_t min_num_params = 0;
            size_t max_num_params = 0;
            String needed_params;

            auto add_mandatory_param = [&](const char * desc) {
                ++min_num_params;
                ++max_num_params;
                needed_params += needed_params.empty() ? "\n" : ",\n";
                needed_params += desc;
            };

            add_mandatory_param("replication factor");
            add_mandatory_param("shards");
            add_mandatory_param("sharding key expression");

            ASTs & engine_args = args.engine_args;
            size_t arg_cnt = engine_args.size();

            if (arg_cnt < min_num_params || arg_cnt > max_num_params)
            {
                String msg;
                msg += "With extended storage definition syntax storage " + args.engine_name + " requires ";

                if (max_num_params)
                {
                    if (min_num_params == max_num_params)
                        msg += toString(min_num_params) + " parameters: ";
                    else
                        msg += toString(min_num_params) + " to " + toString(max_num_params) + " parameters: ";
                    msg += needed_params;
                }
                else
                    msg += "no parameters";

                msg += StorageStreamProperties::getVerboseHelp();

                throw Exception(msg, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
            }

            /// This merging param maybe used as part of sorting key
            std::optional<String> merging_param_key_arg;

            String date_column_name;

            StorageInMemoryMetadata metadata;

            ColumnsDescription columns = args.columns;

            metadata.setColumns(columns);
            metadata.setComment(args.comment);

            ASTPtr partition_by_key;
            if (args.storage_def->partition_by)
                partition_by_key = args.storage_def->partition_by->ptr();

            /// Partition key may be undefined, but despite this we store it's empty
            /// value in partition_key structure. MergeTree checks this case and use
            /// single default partition with name "all".
            metadata.partition_key = KeyDescription::getKeyFromAST(partition_by_key, metadata.columns, args.getContext());

            /// PRIMARY KEY without ORDER BY is allowed and considered as ORDER BY.
            if (!args.storage_def->order_by && args.storage_def->primary_key)
                args.storage_def->set(args.storage_def->order_by, args.storage_def->primary_key->clone());

            if (!args.storage_def->order_by)
                /// proton: starts
                throw Exception(
                    "You must provide an ORDER BY or PRIMARY KEY expression in the stream definition. "
                    "If you don't want this stream to be sorted, use ORDER BY/PRIMARY KEY tuple()",
                    ErrorCodes::BAD_ARGUMENTS);
            /// proton: ends

            /// Get sorting key from engine arguments.
            ///
            /// NOTE: store merging_param_key_arg as additional key column. We do it
            /// before storage creation. After that storage will just copy this
            /// column if sorting key will be changed.
            metadata.sorting_key = KeyDescription::getSortingKeyFromAST(
                args.storage_def->order_by->ptr(), metadata.columns, args.getContext(), merging_param_key_arg);

            /// If primary key explicitly defined, than get it from AST
            if (args.storage_def->primary_key)
            {
                metadata.primary_key
                    = KeyDescription::getKeyFromAST(args.storage_def->primary_key->ptr(), metadata.columns, args.getContext());
            }
            else /// Otherwise we don't have explicit primary key and copy it from order by
            {
                metadata.primary_key
                    = KeyDescription::getKeyFromAST(args.storage_def->order_by->ptr(), metadata.columns, args.getContext());
                /// and set it's definition_ast to nullptr (so isPrimaryKeyDefined()
                /// will return false but hasPrimaryKey() will return true.
                metadata.primary_key.definition_ast = nullptr;
            }

            auto minmax_columns = metadata.getColumnsRequiredForPartitionKey();
            auto primary_key_asts = metadata.primary_key.expression_list_ast->children;
            metadata.minmax_count_projection.emplace(ProjectionDescription::getMinMaxCountProjection(
                args.columns, metadata.partition_key.expression_list_ast, minmax_columns, primary_key_asts, args.getContext()));

            if (args.storage_def->sample_by)
                metadata.sampling_key
                    = KeyDescription::getKeyFromAST(args.storage_def->sample_by->ptr(), metadata.columns, args.getContext());

            if (args.storage_def->ttl_table)
            {
                metadata.table_ttl = TTLTableDescription::getTTLForTableFromAST(
                    args.storage_def->ttl_table->ptr(), metadata.columns, args.getContext(), metadata.primary_key);
            }

            if (args.query.columns_list && args.query.columns_list->indices)
                for (auto & index : args.query.columns_list->indices->children)
                    metadata.secondary_indices.push_back(IndexDescription::getIndexFromAST(index, columns, args.getContext()));

            if (args.query.columns_list && args.query.columns_list->projections)
                for (auto & projection_ast : args.query.columns_list->projections->children)
                {
                    auto projection = ProjectionDescription::getProjectionFromAST(projection_ast, columns, args.getContext());
                    metadata.projections.add(std::move(projection));
                }

            auto constraints = metadata.constraints.getConstraints();
            if (args.query.columns_list && args.query.columns_list->constraints)
                for (auto & constraint : args.query.columns_list->constraints->children)
                    constraints.push_back(constraint);
            metadata.constraints = ConstraintsDescription(constraints);

            auto column_ttl_asts = columns.getColumnTTLs();
            for (const auto & [name, ast] : column_ttl_asts)
            {
                auto new_ttl_entry = TTLDescription::getTTLFromAST(ast, columns, args.getContext(), metadata.primary_key);
                metadata.column_ttls_by_name[name] = new_ttl_entry;
            }

            auto properties = StorageStreamProperties::create(*args.storage_def, args.columns, args.getLocalContext());

            // updates the default storage_settings with settings specified via SETTINGS arg in a query
            if (args.storage_def->settings)
                metadata.settings_changes = args.storage_def->settings->ptr();

            DataTypes data_types = metadata.partition_key.data_types;
            if (!args.attach && !properties->storage_settings->allow_floating_point_partition_key)
            {
                for (size_t i = 0; i < data_types.size(); ++i)
                    if (isFloat(data_types[i]))
                        throw Exception(
                            ErrorCodes::BAD_ARGUMENTS,
                            "Floating point partition key is not supported: {}",
                            metadata.partition_key.column_names[i]);
            }

            return StorageStream::create(
                properties->replication_factor,
                properties->shards,
                properties->sharding_expr,
                args.table_id,
                args.relative_data_path,
                metadata,
                args.attach,
                args.getContext(),
                date_column_name,
                merging_params,
                std::move(properties->storage_settings),
                args.has_force_restore_data_flag);
        },
        features);
}

}
