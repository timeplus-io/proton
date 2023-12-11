#include <Storages/Streaming/StorageKV.h>
#include <Storages/Streaming/StorageStreamProperties.h>
#include <Storages/StorageFactory.h>
#include <Common/ProtonCommon.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/// Storage stream has 4 modes:
/// 1. append only
/// CREATE STREAM append_only_stream(i int) or CREATE STREAM append_only_stream(i int) SETTINGS mode='append';
/// 2. changelog
/// CREATE STREAM append_only_stream(i int) SETTINGS mode='changelog';
/// changelog stream has special `_tp_delta` column -> (i int, _tp_delta int8, ...).
/// The same as append-only stream, it is append-only but with update / delete semantic.
/// Different than append-only stream, during query processing, changelog stream has append/update/delete (retraction) semantic.
/// Different than `keyed changelog` stream, its historical store doesn't do compaction since it doesn't have
/// primary key explicitly specified.
/// 3. changelog kv.
/// CREATE STREAM changelog_kv_stream(i int, k string) PRIMARY KEY k SETTINGS mode='changelog_kv', version='_tp_time';
/// `changelog kv stream` is `changelog stream + primary key`. Users has to specify `PRIMARY KEY` and can't specify `ORDER BY`
/// at the same time or `ORDER BY` shall be exactly the same as `PRIMARY KEY`.
/// Similar to `changelog stream`, `changelog kv stream` has special `_tp_delta` column as well.
/// Different than `changelog stream`, Its historical store will do special compaction
/// according to `_tp_delta` and primary key which eventually only persists the latest snapshot
/// per primary key. The `changelog kv stream` is modeled for CDC data.
/// 4. versioned kv stream
/// CREATE STREAM versioned_stream(i int, k string) PRIMARY KEY k SETTINGS mode='versioned_kv', version='_tp_time', keep_versions=1;
/// Similar to `changelog kv stream`, the historical store of a kv stream does background compaction per key.
/// But it may keep multiple versions per key around. The compaction algorithm is different than `changelog kv`
/// as well since it doesn't have `_tp_delta` special column. During compaction, it sorts the data according
/// to (primary key, the version column) and than do compaction per key according to `keep_versions` setting.
/// By default, kv stream only keeps the latest version around.

void registerStorageKV(StorageFactory & factory)
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
        "KV",
        [](const StorageFactory::Arguments & args) {
            /** * Stream engine arguments : Stream(replication_factor, shards, shard_by_expr)
            * - replication_factor
            * - shards
            * - shard_by_expr

            * Stream engine settings :
            * - mode=changelog
            * - version=_tp_time
            * - logstore=kafka
            * - logstore_cluster_id=<my_cluster>
            * - logstore_partition=<partition>
            * - logstore_request_required_acks=1
            * - logstore_request_timeout_ms=30000
            * - logstore_auto_offset_reset=earliest
            * - logstore_replication_factor=1
            */

            MergeTreeData::MergingParams merging_params;

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
                throw Exception(
                    "You must provide an ORDER BY or PRIMARY KEY expression in the stream definition. "
                    "If you don't want this stream to be sorted, use ORDER BY/PRIMARY KEY tuple()",
                    ErrorCodes::BAD_ARGUMENTS);

            /// Get sorting key from engine arguments.
            ///
            /// NOTE: store merging_param_key_arg as additional key column. We do it
            /// before storage creation. After that storage will just copy this
            /// column if sorting key will be changed.

            auto properties = StorageStreamProperties::create(*args.storage_def, args.columns, args.getLocalContext());

            auto is_regular_storage = [&]() {
                /// Changelog or append mode has ordinary mergetree storage
                return properties->storage_settings->mode.value == ProtonConsts::APPEND_MODE
                    || properties->storage_settings->mode.value == ProtonConsts::CHANGELOG_MODE;
            };

            if (is_regular_storage())
                metadata.sorting_key = KeyDescription::getSortingKeyFromAST(
                        args.storage_def->order_by->ptr(), metadata.columns, args.getContext(), {});
            else
                /// Add version column to sorting key
                metadata.sorting_key = KeyDescription::getSortingKeyFromAST(
                        args.storage_def->order_by->ptr(), metadata.columns, args.getContext(), {properties->storage_settings->version_column.value});

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
            {
                for (auto & projection_ast : args.query.columns_list->projections->children)
                {
                    auto projection = ProjectionDescription::getProjectionFromAST(projection_ast, columns, args.getContext());
                    metadata.projections.add(std::move(projection));
                }
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

            if (is_regular_storage())
            {
                merging_params.mode = MergeTreeData::MergingParams::Ordinary;
            }
            else
            {
                /// Changelog with primary key or kv mode requires version column
                if (properties->storage_settings->mode.value == ProtonConsts::CHANGELOG_KV_MODE)
                {
                    merging_params.mode = MergeTreeData::MergingParams::ChangelogKV;
                    merging_params.sign_column = ProtonConsts::RESERVED_DELTA_FLAG;
                }
                else if (properties->storage_settings->mode.value == ProtonConsts::VERSIONED_KV_MODE)
                {
                    merging_params.mode = MergeTreeData::MergingParams::VersionedKV;
                    merging_params.keep_versions = properties->storage_settings->keep_versions.value;
                }
                else
                    throw Exception(ErrorCodes::SYNTAX_ERROR, "Invalid storage mode='{}'", properties->storage_settings->mode.value);

                merging_params.version_column = properties->storage_settings->version_column.value;

                /// We need make sure version_column is not in primary key
                if (metadata.primary_key.has(merging_params.version_column))
                    throw Exception(ErrorCodes::SYNTAX_ERROR, "Version column as primary key is not supported in Changelog KV or Versioned KV stream");
            }

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

            return StorageKV::create(
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
