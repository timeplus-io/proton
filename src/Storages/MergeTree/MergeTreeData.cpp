#include "Storages/MergeTree/MergeTreeDataPartBuilder.h"
#include <Storages/MergeTree/MergeTreeData.h>

#include <AggregateFunctions/AggregateFunctionCount.h>
#include <Backups/BackupEntryFromImmutableFile.h>
#include <Backups/IBackup.h>
#include <Common/escapeForFileName.h>
#include <Common/Increment.h>
#include <Common/noexcept_scope.h>
#include <Common/quoteString.h>
#include <Common/scope_guard_safe.h>
#include <Common/SimpleIncrement.h>
#include <Common/Stopwatch.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/typeid_cast.h>
#include <Common/CurrentMetrics.h>
#include <Core/QueryProcessingStage.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/hasNullable.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/ObjectUtils.h>
#include <Disks/createVolume.h>
#include <Disks/ObjectStorages/DiskObjectStorage.h>
#include <Disks/TemporaryFileOnDisk.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/Context.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/MergeTreeTransaction.h>
#include <Interpreters/PartLog.h>
#include <Interpreters/TransactionLog.h>
#include <Interpreters/TreeRewriter.h>
#include <IO/copyData.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <IO/S3Common.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/PartLog.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <Interpreters/MergeTreeTransaction.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/TransactionLog.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/convertFieldToType.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTPartition.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Storages/AlterCommands.h>
#include <Storages/MergeTree/checkDataPart.h>
#include <Storages/MutationCommands.h>
#include <Storages/MergeTree/ActiveDataPartSet.h>
#include <Storages/MergeTree/MergeTreeSelectProcessor.h>
#include <Storages/MergeTree/MergeTreeDataPartCompact.h>
#include <Storages/MergeTree/MergeTreeDataPartInMemory.h>
#include <Storages/MergeTree/MergeTreeDataPartWide.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Storages/MergeTree/checkDataPart.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/VirtualColumnUtils.h>

#include <boost/algorithm/string/join.hpp>

#include <ranges>

#include <base/insertAtEnd.h>
#include <base/sort.h>
#include <base/defines.h>
#include <base/isSharedPtrUnique.h>

#include <algorithm>
#include <atomic>
#include <optional>
#include <set>
#include <thread>
#include <typeinfo>
#include <typeindex>
#include <unordered_set>
#include <filesystem>

#include <Poco/Logger.h>
/// proton : starts
#include <Common/ProtonCommon.h>
/// proton : ends


template <>
struct fmt::formatter<DB::DataPartPtr> : fmt::formatter<std::string>
{
    template <typename FormatCtx>
    auto format(const DB::DataPartPtr & part, FormatCtx & ctx) const
    {
        return fmt::formatter<std::string>::format(part->name, ctx);
    }
};


namespace fs = std::filesystem;

namespace ProfileEvents
{
    extern const Event RejectedInserts;
    extern const Event DelayedInserts;
    extern const Event DelayedInsertsMilliseconds;
    extern const Event InsertedWideParts;
    extern const Event InsertedCompactParts;
    extern const Event InsertedInMemoryParts;
    extern const Event MergedIntoWideParts;
    extern const Event MergedIntoCompactParts;
    extern const Event MergedIntoInMemoryParts;
}

namespace CurrentMetrics
{
    extern const Metric DelayedInserts;
    extern const Metric MergeTreePartsLoaderThreads;
    extern const Metric MergeTreePartsLoaderThreadsActive;
    extern const Metric MergeTreePartsCleanerThreads;
    extern const Metric MergeTreePartsCleanerThreadsActive;
}


namespace
{
    constexpr UInt64 RESERVATION_MIN_ESTIMATION_SIZE = 1u * 1024u * 1024u; /// 1MB
}


namespace DB
{

namespace ErrorCodes
{
    extern const int NO_SUCH_DATA_PART;
    extern const int NOT_IMPLEMENTED;
    extern const int DIRECTORY_ALREADY_EXISTS;
    extern const int TOO_MANY_UNEXPECTED_DATA_PARTS;
    extern const int DUPLICATE_DATA_PART;
    extern const int NO_SUCH_COLUMN_IN_STREAM;
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
    extern const int CORRUPTED_DATA;
    extern const int BAD_TYPE_OF_FIELD;
    extern const int BAD_ARGUMENTS;
    extern const int INVALID_PARTITION_VALUE;
    extern const int METADATA_MISMATCH;
    extern const int PART_IS_TEMPORARILY_LOCKED;
    extern const int TOO_MANY_PARTS;
    extern const int INCOMPATIBLE_COLUMNS;
    extern const int BAD_TTL_EXPRESSION;
    extern const int INCORRECT_FILE_NAME;
    extern const int BAD_DATA_PART_NAME;
    extern const int READONLY_SETTING;
    extern const int ABORTED;
    extern const int UNKNOWN_DISK;
    extern const int NOT_ENOUGH_SPACE;
    extern const int ALTER_OF_COLUMN_IS_FORBIDDEN;
    extern const int SUPPORT_IS_DISABLED;
    extern const int TOO_MANY_SIMULTANEOUS_QUERIES;
    extern const int INCORRECT_QUERY;
    extern const int ZERO_COPY_REPLICATION_ERROR;
    extern const int NOT_INITIALIZED;
    extern const int SERIALIZATION_ERROR;
    extern const int NETWORK_ERROR;
    extern const int SOCKET_TIMEOUT;
}

static void checkSampleExpression(const StorageInMemoryMetadata & metadata, bool allow_sampling_expression_not_in_primary_key, bool check_sample_column_is_correct)
{
    if (metadata.sampling_key.column_names.empty())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "There are no columns in sampling expression");

    const auto & pk_sample_block = metadata.getPrimaryKey().sample_block;
    if (!pk_sample_block.has(metadata.sampling_key.column_names[0]) && !allow_sampling_expression_not_in_primary_key)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Sampling expression must be present in the primary key");

    if (!check_sample_column_is_correct)
        return;

    const auto & sampling_key = metadata.getSamplingKey();
    DataTypePtr sampling_column_type = sampling_key.data_types[0];

    bool is_correct_sample_condition = false;
    if (sampling_key.data_types.size() == 1)
    {
        if (typeid_cast<const DataTypeUInt64 *>(sampling_column_type.get()))
            is_correct_sample_condition = true;
        else if (typeid_cast<const DataTypeUInt32 *>(sampling_column_type.get()))
            is_correct_sample_condition = true;
        else if (typeid_cast<const DataTypeUInt16 *>(sampling_column_type.get()))
            is_correct_sample_condition = true;
        else if (typeid_cast<const DataTypeUInt8 *>(sampling_column_type.get()))
            is_correct_sample_condition = true;
    }

    if (!is_correct_sample_condition)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER,
            "Invalid sampling column type in storage parameters: {}. Must be one unsigned integer type",
            sampling_column_type->getName());
}

static Streaming::StorageSemantic toStorageSemantic(const std::string & storage_mode)
{
    if (storage_mode == ProtonConsts::CHANGELOG_MODE)
        return Streaming::StorageSemantic::Changelog;
    else if (storage_mode == ProtonConsts::CHANGELOG_KV_MODE)
        return Streaming::StorageSemantic::ChangelogKV;
    else if (storage_mode == ProtonConsts::VERSIONED_KV_MODE)
        return Streaming::StorageSemantic::VersionedKV;
    else
        return Streaming::StorageSemantic::Append;
}


void MergeTreeData::initializeDirectoriesAndFormatVersion(const std::string & relative_data_path_, bool attach, const std::string & date_column_name, bool need_create_directories)
{
    relative_data_path = relative_data_path_;

    MergeTreeDataFormatVersion min_format_version(0);
    if (date_column_name.empty())
        min_format_version = MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING;

    if (relative_data_path.empty())
        throw Exception(ErrorCodes::INCORRECT_FILE_NAME, "MergeTree storages require data path");

    const auto format_version_path = fs::path(relative_data_path) / MergeTreeData::FORMAT_VERSION_FILE_NAME;
    std::optional<UInt32> read_format_version;

    for (const auto & disk : getDisks())
    {
        if (disk->isBroken())
            continue;

        if (need_create_directories)
        {
            disk->createDirectories(relative_data_path);
            disk->createDirectories(fs::path(relative_data_path) / MergeTreeData::DETACHED_DIR_NAME);
        }

        if (disk->exists(format_version_path))
        {
            auto buf = disk->readFile(format_version_path);
            UInt32 current_format_version{0};
            readIntText(current_format_version, *buf);
            if (!buf->eof())
                throw Exception(ErrorCodes::CORRUPTED_DATA, "Bad version file: {}", fullPath(disk, format_version_path));

            if (!read_format_version.has_value())
                read_format_version = current_format_version;
            else if (*read_format_version != current_format_version)
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                                "Version file on {} contains version {} expected version is {}.",
                                fullPath(disk, format_version_path), current_format_version, *read_format_version);
        }
    }


    // When data path or file not exists, ignore the format_version check
    if (!attach || !read_format_version)
    {
        format_version = min_format_version;

        // try to write to first non-readonly disk
        for (const auto & disk : getStoragePolicy()->getDisks())
        {
            if (disk->isBroken())
               continue;

            /// Write once disk is almost the same as read-only for MergeTree,
            /// since it does not support move, that is required for any
            /// operation over MergeTree, so avoid writing format_version.txt
            /// into it as well, to avoid leaving it after DROP.
            if (!disk->isReadOnly() && !disk->isWriteOnce())
            {
                auto buf = disk->writeFile(format_version_path, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, getContext()->getWriteSettings());
                writeIntText(format_version.toUnderType(), *buf);
                buf->finalize();
                if (getContext()->getSettingsRef().fsync_metadata)
                    buf->sync();
            }

            break;
        }
    }
    else
    {
        format_version = *read_format_version;
    }

    if (format_version < min_format_version)
    {
        if (min_format_version == MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING.toUnderType())
            throw Exception(ErrorCodes::METADATA_MISMATCH, "MergeTree data format version on disk doesn't support custom partitioning");
    }
}

MergeTreeData::MergeTreeData(
    const StorageID & table_id_,
    const StorageInMemoryMetadata & metadata_,
    ContextMutablePtr context_,
    const String & date_column_name,
    const MergingParams & merging_params_,
    std::unique_ptr<MergeTreeSettings> storage_settings_,
    bool require_part_metadata_,
    bool attach,
    UInt32 shard_, /// proton : added
    BrokenPartCallback broken_part_callback_)
    : IStorage(table_id_)
    , WithMutableContext(context_->getGlobalContext())
    , format_version(date_column_name.empty() ? MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING : MERGE_TREE_DATA_OLD_FORMAT_VERSION)
    , merging_params(merging_params_)
    , require_part_metadata(require_part_metadata_)
    , broken_part_callback(broken_part_callback_)
    , log(table_id_.getNameForLogs())
    , storage_settings(std::move(storage_settings_))
    , pinned_part_uuids(std::make_shared<PinnedPartUUIDs>())
    , data_parts_by_info(data_parts_indexes.get<TagByInfo>())
    , data_parts_by_state_and_info(data_parts_indexes.get<TagByStateAndInfo>())
    , parts_mover(this)
    , use_metadata_cache(getSettings()->use_metadata_cache)
    /// proton: starts.
    , shard(shard_)
    /// proton: ends.
{
    data_stream_semantic.attachStorageSemantic(toStorageSemantic(storage_settings.get()->mode.value));
    /// proton : ends

    context_->getGlobalContext()->initializeBackgroundExecutorsIfNeeded();

    auto settings = getSettings();

    allow_nullable_key = attach || settings->allow_nullable_key;

    /// Check sanity of MergeTreeSettings. Only when table is created.
    if (!attach)
        settings->sanityCheck(getContext()->getSettingsRef());

    if (!date_column_name.empty())
    {
        try
        {
            checkPartitionKeyAndInitMinMax(metadata_.partition_key);
            setProperties(metadata_, metadata_, attach);
            if (minmax_idx_date_column_pos == -1)
                throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "Could not find Date column");
        }
        catch (Exception & e)
        {
            /// Better error message.
            e.addMessage("(while initializing the partition key from date column " + backQuote(date_column_name) + ")");
            throw;
        }
    }
    else
    {
        is_custom_partitioned = true;
        checkPartitionKeyAndInitMinMax(metadata_.partition_key);
    }
    setProperties(metadata_, metadata_, attach);

    /// NOTE: using the same columns list as is read when performing actual merges.
    merging_params.check(metadata_);

    if (metadata_.sampling_key.definition_ast != nullptr)
    {
        /// This is for backward compatibility.
        checkSampleExpression(metadata_, attach || settings->compatibility_allow_sampling_expression_not_in_primary_key,
                              settings->check_sample_column_is_correct && !attach);
    }

    checkColumnFilenamesForCollision(metadata_.getColumns(), *settings, !attach);
    checkTTLExpressions(metadata_, metadata_);

    /// proton : starts
    /// MergeTreeData was initiated in the following 3 different ways
    /// 1. StorageMergeTree  inherits MergeTreeData
    /// 2. StreamShardStore  composes StorageMergeTree
    /// 3. StorageStream     inherits MergeTreeData (facade)
    ///                      composes StreamShardStores
    /// 4. MaterializedView  composes StorageStream
    ///
    /// For case 1), it is CREATE STREAM ... ENGINE=MergeTree case which is not replicated
    /// replication factor is always 1, but could have multiple shards in future. The ShardReplica
    /// shall be always `shard = 0 && replica = 1` for now.
    /// For case 2), for now, there are duplicated validation as in 3) which we shall be able to avoid
    /// The shard passed in shall always be inited and valid.
    /// For case 3), since `StorageStream` is just acting like a facade class which delegates the
    /// real work to its underlying `StreamShardStore`, the initialization of its parent class (MergeTreeData)
    /// shall only do validation check without touching file system. The shard parameter passed in will be
    /// unresolved (Nulls::NullShardID).
    /// Note, previously when MergeTreeData gets inited, it always means it has backing historical storage on the
    /// current node. However in cluster env, there is not true anymore, which is case 3). When we create a replicated stream,
    /// we will need do initiate a temporary `StorageStream` storage object to do validation instance on the initiator node.
    /// At this specific time, we haven't resolve the placement assignments etc yet and `shard` will be unresolved.
    /// Ideally we can separate the streaming validation process and the real StorageStream object create process, but for now,
    /// this code are merged together.
    /// For other cases like pure in-memory stream, we don't initiate MergeTreeData, for which
    /// we will check / handle at up layer.
    /// For pure proxy stream, we don't initiate MergeTreeData neither, for which we will also check / handle at up layer.
    if (shard == cluster::Nulls::NullShardID)
        /// This is for case 2) either during stream creation process and placements are not resolved yet.
        /// or it is pure facade
        /// After we finished the validation of the settings, we don't need create version_file etc
        return;

    /// We like to defer this initialization if it is neither a pure facade nor virtual
    background_operations_assignee = std::make_unique<BackgroundJobsAssignee>(*this, BackgroundJobsAssignee::Type::DataProcessing, getContext());
    background_moves_assignee = std::make_unique<BackgroundJobsAssignee>(*this, BackgroundJobsAssignee::Type::Moving, getContext());
    /// proton : ends

    String reason;
    if (!canUsePolymorphicParts(*settings, &reason) && !reason.empty())
        LOG_WARNING(log, "{} Settings 'min_rows_for_wide_part', 'min_bytes_for_wide_part', "
            "'min_rows_for_compact_part' and 'min_bytes_for_compact_part' will be ignored.", reason);

#if !USE_ROCKSDB
    if (use_metadata_cache)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't use merge tree metadata cache if proton was compiled without rocksdb");
#endif

    common_assignee_trigger = [this] (bool delay) noexcept
    {
        if (delay)
            background_operations_assignee->postpone();
        else
            background_operations_assignee->trigger();
    };

    moves_assignee_trigger = [this] (bool delay) noexcept
    {
        if (delay)
            background_moves_assignee->postpone();
        else
            background_moves_assignee->trigger();
    };
}

StoragePolicyPtr MergeTreeData::getStoragePolicy() const
{
    auto settings = getSettings();
    const auto & context = getContext();

    StoragePolicyPtr storage_policy;

    if (settings->disk.changed)
        storage_policy = context->getStoragePolicyFromDisk(settings->disk);
    else
        storage_policy = context->getStoragePolicy(settings->storage_policy);

    return storage_policy;
}

bool MergeTreeData::supportsFinal() const
{
    return merging_params.mode == MergingParams::Collapsing
        || merging_params.mode == MergingParams::Summing
        || merging_params.mode == MergingParams::Aggregating
        || merging_params.mode == MergingParams::VersionedKV
        || merging_params.mode == MergingParams::Graphite
        || merging_params.mode == MergingParams::ChangelogKV;
}

static void checkKeyExpression(const ExpressionActions & expr, const Block & sample_block, const String & key_name, bool allow_nullable_key)
{
    if (expr.hasArrayJoin())
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "{} key cannot contain array joins", key_name);

    try
    {
        expr.assertDeterministic();
    }
    catch (Exception & e)
    {
        e.addMessage(fmt::format("for {} key", key_name));
        throw;
    }

    for (const ColumnWithTypeAndName & element : sample_block)
    {
        const ColumnPtr & column = element.column;
        if (column && (isColumnConst(*column) || column->isDummy()))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "{} key cannot contain constants", key_name);

        if (!allow_nullable_key && hasNullable(element.type))
            throw Exception(
                            ErrorCodes::ILLEGAL_COLUMN,
                            "{} key contains nullable columns, "
                            "but merge tree setting `allow_nullable_key` is disabled", key_name);
    }
}

void MergeTreeData::checkProperties(
    const StorageInMemoryMetadata & new_metadata, const StorageInMemoryMetadata & old_metadata, bool attach) const
{
    if (!new_metadata.sorting_key.definition_ast)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ORDER BY cannot be empty");

    KeyDescription new_sorting_key = new_metadata.sorting_key;
    KeyDescription new_primary_key = new_metadata.primary_key;

    size_t sorting_key_size = new_sorting_key.column_names.size();
    size_t primary_key_size = new_primary_key.column_names.size();
    if (primary_key_size > sorting_key_size)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Primary key must be a prefix of the sorting key, but its length: "
            "{} is greater than the sorting key length: {}", primary_key_size, sorting_key_size);

    /// proton : starts
    if (merging_params.mode == MergingParams::ChangelogKV || merging_params.mode == MergingParams::VersionedKV)
    {
        /// We added `version` column to sorting key, so +1 here
        if (primary_key_size + 1 != sorting_key_size)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "Primary key must be the same as sorting key for changelog_kv or versioned_kv stream");
    }
    /// proton : ends

    NameSet primary_key_columns_set;

    for (size_t i = 0; i < sorting_key_size; ++i)
    {
        const String & sorting_key_column = new_sorting_key.column_names[i];

        if (i < primary_key_size)
        {
            const String & pk_column = new_primary_key.column_names[i];
            if (pk_column != sorting_key_column)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "Primary key must be a prefix of the sorting key, "
                                "but the column in the position {} is {}", i, sorting_key_column +", not " + pk_column);

            if (!primary_key_columns_set.emplace(pk_column).second)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Primary key contains duplicate columns");

        }
    }

    auto all_columns = new_metadata.columns.getAllPhysical();

    /// Order by check AST
    if (old_metadata.hasSortingKey())
    {
        /// This is ALTER, not CREATE/ATTACH TABLE. Let us check that all new columns used in the sorting key
        /// expression have just been added (so that the sorting order is guaranteed to be valid with the new key).

        Names new_primary_key_columns = new_primary_key.column_names;
        Names new_sorting_key_columns = new_sorting_key.column_names;

        ASTPtr added_key_column_expr_list = std::make_shared<ASTExpressionList>();
        const auto & old_sorting_key_columns = old_metadata.getSortingKeyColumns();
        for (size_t new_i = 0, old_i = 0; new_i < sorting_key_size; ++new_i)
        {
            if (old_i < old_sorting_key_columns.size())
            {
                if (new_sorting_key_columns[new_i] != old_sorting_key_columns[old_i])
                    added_key_column_expr_list->children.push_back(new_sorting_key.expression_list_ast->children[new_i]);
                else
                    ++old_i;
            }
            else
                added_key_column_expr_list->children.push_back(new_sorting_key.expression_list_ast->children[new_i]);
        }

        if (!added_key_column_expr_list->children.empty())
        {
            auto syntax = TreeRewriter(getContext()).analyze(added_key_column_expr_list, all_columns);
            Names used_columns = syntax->requiredSourceColumns();

            NamesAndTypesList deleted_columns;
            NamesAndTypesList added_columns;
            old_metadata.getColumns().getAllPhysical().getDifference(all_columns, deleted_columns, added_columns);

            for (const String & col : used_columns)
            {
                if (!added_columns.contains(col) || deleted_columns.contains(col))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                    "Existing column {} is used in the expression that was added to the sorting key. "
                                    "You can add expressions that use only the newly added columns",
                                    backQuoteIfNeed(col));

                if (new_metadata.columns.getDefaults().contains(col))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                    "Newly added column {} has a default expression, so adding expressions that use "
                                    "it to the sorting key is forbidden", backQuoteIfNeed(col));
            }
        }
    }

    if (!new_metadata.secondary_indices.empty())
    {
        std::unordered_set<String> indices_names;

        for (const auto & index : new_metadata.secondary_indices)
        {

            MergeTreeIndexFactory::instance().validate(index, attach);

            if (indices_names.find(index.name) != indices_names.end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Index with name {} already exists", backQuote(index.name));

            indices_names.insert(index.name);
        }
    }

    if (!new_metadata.projections.empty())
    {
        std::unordered_set<String> projections_names;

        for (const auto & projection : new_metadata.projections)
        {
            if (projections_names.find(projection.name) != projections_names.end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Projection with name {} already exists", backQuote(projection.name));

            projections_names.insert(projection.name);
        }
    }

    checkKeyExpression(*new_sorting_key.expression, new_sorting_key.sample_block, "Sorting", allow_nullable_key);
}

void MergeTreeData::setProperties(const StorageInMemoryMetadata & new_metadata, const StorageInMemoryMetadata & old_metadata, bool attach)
{
    checkProperties(new_metadata, old_metadata, attach);
    setInMemoryMetadata(new_metadata);
}

namespace
{

ExpressionActionsPtr getCombinedIndicesExpression(
    const KeyDescription & key,
    const MergeTreeIndices & indices,
    const ColumnsDescription & columns,
    ContextPtr context)
{
    ASTPtr combined_expr_list = key.expression_list_ast->clone();

    for (const auto & index : indices)
        for (const auto & index_expr : index->index.expression_list_ast->children)
            combined_expr_list->children.push_back(index_expr->clone());

    auto syntax_result = TreeRewriter(context).analyze(combined_expr_list, columns.getAllPhysical());
    return ExpressionAnalyzer(combined_expr_list, syntax_result, context).getActions(false);
}

}

ExpressionActionsPtr MergeTreeData::getMinMaxExpr(const KeyDescription & partition_key, const ExpressionActionsSettings & settings)
{
    NamesAndTypesList partition_key_columns;
    if (!partition_key.column_names.empty())
        partition_key_columns = partition_key.expression->getRequiredColumnsWithTypes();

    return std::make_shared<ExpressionActions>(std::make_shared<ActionsDAG>(partition_key_columns), settings);
}

Names MergeTreeData::getMinMaxColumnsNames(const KeyDescription & partition_key)
{
    if (!partition_key.column_names.empty())
        return partition_key.expression->getRequiredColumns();
    return {};
}

DataTypes MergeTreeData::getMinMaxColumnsTypes(const KeyDescription & partition_key)
{
    if (!partition_key.column_names.empty())
        return partition_key.expression->getRequiredColumnsWithTypes().getTypes();
    return {};
}

ExpressionActionsPtr
MergeTreeData::getPrimaryKeyAndSkipIndicesExpression(const StorageMetadataPtr & metadata_snapshot, const MergeTreeIndices & indices) const
{
    return getCombinedIndicesExpression(metadata_snapshot->getPrimaryKey(), indices, metadata_snapshot->getColumns(), getContext());
}

ExpressionActionsPtr
MergeTreeData::getSortingKeyAndSkipIndicesExpression(const StorageMetadataPtr & metadata_snapshot, const MergeTreeIndices & indices) const
{
    return getCombinedIndicesExpression(metadata_snapshot->getSortingKey(), indices, metadata_snapshot->getColumns(), getContext());
}


void MergeTreeData::checkPartitionKeyAndInitMinMax(const KeyDescription & new_partition_key)
{
    if (new_partition_key.expression_list_ast->children.empty())
        return;

    checkKeyExpression(*new_partition_key.expression, new_partition_key.sample_block, "Partition", allow_nullable_key);

    /// Add all columns used in the partition key to the min-max index.
    DataTypes minmax_idx_columns_types = getMinMaxColumnsTypes(new_partition_key);

    /// Try to find the date column in columns used by the partition key (a common case).
    /// If there are no - DateTime or DateTime64 would also suffice.

    bool has_date_column = false;
    bool has_datetime_column = false;

    for (size_t i = 0; i < minmax_idx_columns_types.size(); ++i)
    {
        if (isDate(minmax_idx_columns_types[i]))
        {
            if (!has_date_column)
            {
                minmax_idx_date_column_pos = i;
                has_date_column = true;
            }
            else
            {
                /// There is more than one Date column in partition key and we don't know which one to choose.
                minmax_idx_date_column_pos = -1;
            }
        }
    }
    if (!has_date_column)
    {
        for (size_t i = 0; i < minmax_idx_columns_types.size(); ++i)
        {
            if (isDateTime(minmax_idx_columns_types[i])
                || isDateTime64(minmax_idx_columns_types[i])
            )
            {
                if (!has_datetime_column)
                {
                    minmax_idx_time_column_pos = i;
                    has_datetime_column = true;
                }
                else
                {
                    /// There is more than one DateTime column in partition key and we don't know which one to choose.
                    minmax_idx_time_column_pos = -1;
                }
            }
        }
    }
}


void MergeTreeData::checkTTLExpressions(const StorageInMemoryMetadata & new_metadata, const StorageInMemoryMetadata & old_metadata) const
{
    auto new_column_ttls = new_metadata.column_ttls_by_name;

    if (!new_column_ttls.empty())
    {
        NameSet columns_ttl_forbidden;

        if (old_metadata.hasPartitionKey())
            for (const auto & col : old_metadata.getColumnsRequiredForPartitionKey())
                columns_ttl_forbidden.insert(col);

        if (old_metadata.hasSortingKey())
            for (const auto & col : old_metadata.getColumnsRequiredForSortingKey())
                columns_ttl_forbidden.insert(col);

        for (const auto & [name, ttl_description] : new_column_ttls)
        {
            if (columns_ttl_forbidden.contains(name))
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Trying to set TTL for key column {}", name);
        }
    }
    auto new_table_ttl = new_metadata.table_ttl;

    if (new_table_ttl.definition_ast)
    {
        for (const auto & move_ttl : new_table_ttl.move_ttl)
        {
            if (!move_ttl.if_exists && !getDestinationForMoveTTL(move_ttl))
            {
                if (move_ttl.destination_type == DataDestinationType::DISK)
                    throw Exception(ErrorCodes::BAD_TTL_EXPRESSION,
                                    "No such disk {} for given storage policy", backQuote(move_ttl.destination_name));
                else
                    throw Exception(ErrorCodes::BAD_TTL_EXPRESSION,
                                    "No such volume {} for given storage policy", backQuote(move_ttl.destination_name));
            }
        }
    }
}


void MergeTreeData::checkStoragePolicy(const StoragePolicyPtr & new_storage_policy) const
{
    const auto old_storage_policy = getStoragePolicy();
    old_storage_policy->checkCompatibleWith(new_storage_policy);
}


void MergeTreeData::MergingParams::check(const StorageInMemoryMetadata & metadata) const
{
    const auto columns = metadata.getColumns().getAllPhysical();

    if (!sign_column.empty() && mode != MergingParams::Collapsing && mode != MergingParams::ChangelogKV)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Sign column cannot be specified in modes except Collapsing or Changelog.");

    if (!version_column.empty() && mode != MergingParams::VersionedKV && mode != MergingParams::ChangelogKV)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Version column cannot be specified in modes except KV or Changelog.");

    if (!columns_to_sum.empty() && mode != MergingParams::Summing)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "List of columns to sum for the current engine cannot be specified in all modes except Summing.");

    /// Check that if the sign column is needed, it exists and is of type Int8.
    auto check_sign_column = [this, & columns](bool is_optional, const std::string & storage)
    {
        if (sign_column.empty())
        {
            if (is_optional)
                return;

            throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error: Sign column for storage {} is empty", storage);
        }

        bool miss_column = true;
        for (const auto & column : columns)
        {
            if (column.name == sign_column)
            {
                if (!typeid_cast<const DataTypeInt8 *>(column.type.get()))
                    throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "Sign column ({}) for storage {} must have type int8. "
                            "Provided column of type {}.", sign_column, storage, column.type->getName());
                miss_column = false;
                break;
            }
        }
        if (miss_column)
            throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_STREAM, "Sign column {} does not exist in stream declaration.", sign_column);
    };

    /// that if the version_column column is needed, it exists and is of unsigned integer type.
    auto check_version_column = [this, & columns](bool is_optional, const std::string & storage)
    {
        if (version_column.empty())
        {
            if (is_optional)
                return;

            throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error: Version column for storage {} is empty", storage);
        }

        bool miss_column = true;
        for (const auto & column : columns)
        {
            if (column.name == version_column)
            {
                if (!column.type->canBeUsedAsVersion())
                    throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD,
                                    "The column {} cannot be used as a version column for storage {} because it is "
                                    "of type {} (must be of an integer type or of type date/datetime/datetime64)",
                                    version_column, storage, column.type->getName());
                miss_column = false;
                break;
            }
        }
        if (miss_column)
            throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_STREAM, "Version column {} does not exist in stream declaration.", version_column);
    };

    if (mode == MergingParams::Collapsing)
        check_sign_column(false, "CollapsingMergeTree");

    if (mode == MergingParams::Summing)
    {
        /// If columns_to_sum are set, then check that such columns exist.
        for (const auto & column_to_sum : columns_to_sum)
        {
            auto check_column_to_sum_exists = [& column_to_sum](const NameAndTypePair & name_and_type)
            {
                return column_to_sum == Nested::extractTableName(name_and_type.name);
            };
            if (columns.end() == std::find_if(columns.begin(), columns.end(), check_column_to_sum_exists))
                throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_STREAM,
                                "Column {} listed in columns to sum does not exist in stream declaration.",
                                column_to_sum);
        }

        /// Check that summing columns are not in partition key.
        if (metadata.isPartitionKeyDefined())
        {
            auto partition_key_columns = metadata.getPartitionKey().column_names;

            Names names_intersection;
            std::set_intersection(columns_to_sum.begin(), columns_to_sum.end(),
                                  partition_key_columns.begin(), partition_key_columns.end(),
                                  std::back_inserter(names_intersection));

            if (!names_intersection.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Columns: {} listed both in columns to sum and in partition key. "
                "That is not allowed.", boost::algorithm::join(names_intersection, ", "));
        }
    }

    /// proton : starts. Require version column
    if (mode == MergingParams::VersionedKV)
        check_version_column(false, "KV");

    if (mode == MergingParams::ChangelogKV)
    {
        check_sign_column(false, "Changelog");
        check_version_column(false, "Changelog");
    }
    /// proton : ends.

    /// TODO Checks for Graphite mode.
}


DataTypePtr MergeTreeData::getPartitionValueType() const
{
    DataTypePtr partition_value_type;
    auto partition_types = getInMemoryMetadataPtr()->partition_key.sample_block.getDataTypes();
    if (partition_types.empty())
        partition_value_type = std::make_shared<DataTypeUInt8>();
    else
        partition_value_type = std::make_shared<DataTypeTuple>(std::move(partition_types));
    return partition_value_type;
}


Block MergeTreeData::getSampleBlockWithVirtualColumns() const
{
    DataTypePtr partition_value_type = getPartitionValueType();
    return {
        ColumnWithTypeAndName(ColumnString::create(), std::make_shared<DataTypeString>(), "_part"),
        ColumnWithTypeAndName(ColumnString::create(), std::make_shared<DataTypeString>(), "_partition_id"),
        ColumnWithTypeAndName(ColumnUUID::create(), std::make_shared<DataTypeUUID>(), "_part_uuid"),
        ColumnWithTypeAndName(partition_value_type->createColumn(), partition_value_type, "_partition_value")};
}


Block MergeTreeData::getBlockWithVirtualPartColumns(const MergeTreeData::DataPartsVector & parts, bool one_part, bool ignore_empty) const
{
    auto block = getSampleBlockWithVirtualColumns();
    MutableColumns columns = block.mutateColumns();

    auto & part_column = columns[0];
    auto & partition_id_column = columns[1];
    auto & part_uuid_column = columns[2];
    auto & partition_value_column = columns[3];

    bool has_partition_value = typeid_cast<const ColumnTuple *>(partition_value_column.get());
    for (const auto & part_or_projection : parts)
    {
        if (ignore_empty && part_or_projection->isEmpty())
            continue;
        const auto * part = part_or_projection->isProjectionPart() ? part_or_projection->getParentPart() : part_or_projection.get();
        part_column->insert(part->name);
        partition_id_column->insert(part->info.partition_id);
        part_uuid_column->insert(part->uuid);
        Tuple tuple(part->partition.value.begin(), part->partition.value.end());
        if (has_partition_value)
            partition_value_column->insert(tuple);

        if (one_part)
        {
            part_column = ColumnConst::create(std::move(part_column), 1);
            partition_id_column = ColumnConst::create(std::move(partition_id_column), 1);
            part_uuid_column = ColumnConst::create(std::move(part_uuid_column), 1);
            if (has_partition_value)
                partition_value_column = ColumnConst::create(std::move(partition_value_column), 1);
            break;
        }
    }

    block.setColumns(std::move(columns));
    if (!has_partition_value)
        block.erase("_partition_value");
    return block;
}


std::optional<UInt64> MergeTreeData::totalRowsByPartitionPredicateImpl(
    const SelectQueryInfo & query_info, ContextPtr local_context, const DataPartsVector & parts) const
{
    if (parts.empty())
        return 0u;
    auto metadata_snapshot = getInMemoryMetadataPtr();
    ASTPtr expression_ast;
    Block virtual_columns_block = getBlockWithVirtualPartColumns(parts, true /* one_part */);

    // Generate valid expressions for filtering
    bool valid = VirtualColumnUtils::prepareFilterBlockWithQuery(query_info.query, local_context, virtual_columns_block, expression_ast);

    PartitionPruner partition_pruner(metadata_snapshot, query_info, local_context, true /* strict */);
    if (partition_pruner.isUseless() && !valid)
        return {};

    std::unordered_set<String> part_values;
    if (valid && expression_ast)
    {
        virtual_columns_block = getBlockWithVirtualPartColumns(parts, false /* one_part */);
        VirtualColumnUtils::filterBlockWithQuery(query_info.query, virtual_columns_block, local_context, expression_ast);
        part_values = VirtualColumnUtils::extractSingleValueFromBlock<String>(virtual_columns_block, "_part");
        if (part_values.empty())
            return 0;
    }
    // At this point, empty `part_values` means all parts.

    size_t res = 0;
    for (const auto & part : parts)
    {
        if ((part_values.empty() || part_values.contains(part->name)) && !partition_pruner.canBePruned(*part))
            res += part->rows_count;
    }
    return res;
}

String MergeTreeData::MergingParams::getModeName() const
{
    switch (mode)
    {
        case Ordinary:      return "";
        case Collapsing:    return "Collapsing";
        case Summing:       return "Summing";
        case Aggregating:   return "Aggregating";
        case VersionedKV:   return "Replacing";
        case Graphite:      return "Graphite";
        case ChangelogKV:   return "VersionedCollapsing";
    }

    __builtin_unreachable();
}

Int64 MergeTreeData::getMaxBlockNumber() const
{
    auto lock = lockParts();

    Int64 max_block_num = 0;
    for (const DataPartPtr & part : data_parts_by_info)
        max_block_num = std::max({max_block_num, part->info.max_block, part->info.mutation});

    return max_block_num;
}

void MergeTreeData::PartLoadingTree::add(const MergeTreePartInfo & info, const String & name, const DiskPtr & disk)
{
    auto & current_ptr = root_by_partition[info.partition_id];
    if (!current_ptr)
        current_ptr = std::make_shared<Node>(MergeTreePartInfo{}, "", disk);

    auto * current = current_ptr.get();
    while (true)
    {
        auto it = current->children.lower_bound(info);
        if (it != current->children.begin())
        {
            auto prev = std::prev(it);
            const auto & prev_info = prev->first;

            if (prev_info.contains(info))
            {
                current = prev->second.get();
                continue;
            }
            else if (!prev_info.isDisjoint(info))
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Part {} intersects previous part {}. It is a bug!",
                    name, prev->second->name);
            }
        }

        if (it != current->children.end())
        {
            const auto & next_info = it->first;

            if (next_info.contains(info))
            {
                current = it->second.get();
                continue;
            }
            else if (!next_info.isDisjoint(info))
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Part {} intersects next part {}. It is a bug!",
                    name, it->second->name);
            }
        }

        current->children.emplace(info, std::make_shared<Node>(info, name, disk));
        break;
    }
}

template <typename Func>
void MergeTreeData::PartLoadingTree::traverse(bool recursive, Func && func)
{
    std::function<void(const NodePtr &)> traverse_impl = [&](const auto & node)
    {
        func(node);
        if (recursive)
            for (const auto & [_, child] : node->children)
                traverse_impl(child);
    };

    for (const auto & elem : root_by_partition)
        for (const auto & [_, node] : elem.second->children)
            traverse_impl(node);
}

MergeTreeData::PartLoadingTree
MergeTreeData::PartLoadingTree::build(PartLoadingInfos nodes)
{
    std::sort(nodes.begin(), nodes.end(), [](const auto & lhs, const auto & rhs)
    {
        return std::tie(lhs.info.level, lhs.info.mutation) > std::tie(rhs.info.level, rhs.info.mutation);
    });

    PartLoadingTree tree;
    for (const auto & [info, name, disk] : nodes)
        tree.add(info, name, disk);
    return tree;
}

static std::optional<size_t> calculatePartSizeSafe(
    const MergeTreeData::DataPartPtr & part, const LoggerPtr & log)
{
    try
    {
        return part->getDataPartStorage().calculateTotalSizeOnDisk();
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("while calculating part size {} on path {}",
            part->name, part->getDataPartStorage().getRelativePath()));
        return {};
    }
}

static void preparePartForRemoval(const MergeTreeMutableDataPartPtr & part)
{
    part->remove_time.store(part->modification_time, std::memory_order_relaxed);
    auto creation_csn = part->version.creation_csn.load(std::memory_order_relaxed);
    if (creation_csn != Tx::RolledBackCSN && creation_csn != Tx::PrehistoricCSN && !part->version.isRemovalTIDLocked())
    {
        /// It's possible that covering part was created without transaction,
        /// but if covered part was created with transaction (i.e. creation_tid is not prehistoric),
        /// then it must have removal tid in metadata file.
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Data part {} is Outdated and has creation TID {} and CSN {}, "
                        "but does not have removal tid. It's a bug or a result of manual intervention.",
                        part->name, part->version.creation_tid, creation_csn);
    }

    /// Explicitly set removal_tid_lock for parts w/o transaction (i.e. w/o txn_version.txt)
    /// to avoid keeping part forever (see VersionMetadata::canBeRemoved())
    if (!part->version.isRemovalTIDLocked())
    {
        TransactionInfoContext transaction_context{part->storage.getStorageID(), part->name};
        part->version.lockRemovalTID(Tx::PrehistoricTID, transaction_context);
    }
}

MergeTreeData::LoadPartResult MergeTreeData::loadDataPart(
    const MergeTreePartInfo & part_info,
    const String & part_name,
    const DiskPtr & part_disk_ptr,
    MergeTreeDataPartState to_state,
    std::mutex & part_loading_mutex)
{
    LOG_TRACE(log, "Loading {} part {} from disk {}", magic_enum::enum_name(to_state), part_name, part_disk_ptr->getName());

    LoadPartResult res;
    auto single_disk_volume = std::make_shared<SingleDiskVolume>("volume_" + part_name, part_disk_ptr, 0);
    auto data_part_storage = std::make_shared<DataPartStorageOnDiskFull>(single_disk_volume, relative_data_path, part_name);

    res.part = getDataPartBuilder(part_name, single_disk_volume, part_name)
        .withPartInfo(part_info)
        .withPartFormatFromDisk()
        .build();

    String part_path = fs::path(relative_data_path) / part_name;
    String marker_path = fs::path(part_path) / IMergeTreeDataPart::DELETE_ON_DESTROY_MARKER_FILE_NAME_DEPRECATED;

    if (part_disk_ptr->exists(marker_path))
    {
        /// NOTE: getBytesOnDisk() cannot be used here, since it may be zero if checksums.txt does not exist.
        res.size_of_part = calculatePartSizeSafe(res.part, log.load());
        res.is_broken = true;

        auto part_size_str = res.size_of_part ? formatReadableSizeWithBinarySuffix(*res.size_of_part) : "failed to calculate size";

        LOG_WARNING(log,
            "Detaching stale part {} (size: {}), which should have been deleted after a move. "
            "That can only happen after unclean restart of ClickHouse after move of a part having an operation blocking that stale copy of part.",
            res.part->getDataPartStorage().getFullPath(), part_size_str);

        return res;
    }

    try
    {
        res.part->loadColumnsChecksumsIndexes(require_part_metadata, true);
    }
    catch (...)
    {
        /// Don't count the part as broken if there was a retryalbe error
        /// during loading, such as "not enough memory" or network error.
        if (isRetryableException(std::current_exception()))
            throw;

        res.is_broken = true;
        tryLogCurrentException(log, fmt::format("while loading part {} on path {}", res.part->name, part_path));
    }

    /// Ignore broken parts that can appear as a result of hard server restart.
    if (res.is_broken)
    {
        res.size_of_part = calculatePartSizeSafe(res.part, log.load());
        auto part_size_str = res.size_of_part ? formatReadableSizeWithBinarySuffix(*res.size_of_part) : "failed to calculate size";

        LOG_ERROR(log,
            "Detaching broken part {}{} (size: {}). "
            "If it happened after update, it is likely because of backward incompatibility. "
            "You need to resolve this manually",
            getFullPathOnDisk(part_disk_ptr), part_name, part_size_str);

        return res;
    }

    res.part->modification_time = part_disk_ptr->getLastModified(fs::path(relative_data_path) / part_name).epochTime();
    res.part->loadVersionMetadata();

    if (res.part->wasInvolvedInTransaction())
    {
        /// Check if CSNs were written after committing transaction, update and write if needed.
        bool version_updated = false;
        auto & version = res.part->version;
        chassert(!version.creation_tid.isEmpty());

        if (!res.part->version.creation_csn)
        {
            auto min = TransactionLog::getCSN(res.part->version.creation_tid);
            if (!min)
            {
                /// Transaction that created this part was not committed. Remove part.
                TransactionLog::assertTIDIsNotOutdated(res.part->version.creation_tid);
                min = Tx::RolledBackCSN;
            }

            LOG_TRACE(log, "Will fix version metadata of {} after unclean restart: part has creation_tid={}, setting creation_csn={}",
                        res.part->name, res.part->version.creation_tid, min);

            version.creation_csn = min;
            version_updated = true;
        }

        if (!version.removal_tid.isEmpty() && !version.removal_csn)
        {
            auto max = TransactionLog::getCSN(version.removal_tid);
            if (max)
            {
                LOG_TRACE(log, "Will fix version metadata of {} after unclean restart: part has removal_tid={}, setting removal_csn={}",
                            res.part->name, version.removal_tid, max);
                version.removal_csn = max;
            }
            else
            {
                TransactionLog::assertTIDIsNotOutdated(version.removal_tid);
                /// Transaction that tried to remove this part was not committed. Clear removal_tid.
                LOG_TRACE(log, "Will fix version metadata of {} after unclean restart: clearing removal_tid={}",
                            res.part->name, version.removal_tid);
                version.unlockRemovalTID(version.removal_tid, TransactionInfoContext{getStorageID(), res.part->name});
            }

            version_updated = true;
        }

        /// Sanity checks
        bool csn_order = !version.removal_csn || version.creation_csn <= version.removal_csn || version.removal_csn == Tx::PrehistoricCSN;
        bool min_start_csn_order = version.creation_tid.start_csn <= version.creation_csn;
        bool max_start_csn_order = version.removal_tid.start_csn <= version.removal_csn;
        bool creation_csn_known = version.creation_csn;
        if (!csn_order || !min_start_csn_order || !max_start_csn_order || !creation_csn_known)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Part {} has invalid version metadata: {}", res.part->name, version.toString());

        if (version_updated)
            res.part->storeVersionMetadata(/* force */ true);

        /// Deactivate part if creation was not committed or if removal was.
        if (version.creation_csn == Tx::RolledBackCSN || version.removal_csn)
        {
            preparePartForRemoval(res.part);
            to_state = DataPartState::Outdated;
        }
    }

    res.part->setState(to_state);

    DataPartIteratorByInfo it;
    bool inserted;

    {
        std::lock_guard lock(part_loading_mutex);
        std::tie(it, inserted) = data_parts_indexes.insert(res.part);
    }

    /// Remove duplicate parts with the same checksum.
    if (!inserted)
    {
        if ((*it)->checksums.getTotalChecksumHex() == res.part->checksums.getTotalChecksumHex())
        {
            LOG_ERROR(log, "Remove duplicate part {}", data_part_storage->getFullPath());
            res.part->is_duplicate = true;
            return res;
        }

        throw Exception(ErrorCodes::DUPLICATE_DATA_PART, "Part {} already exists but with different checksums", res.part->name);
    }

    if (to_state == DataPartState::Active)
        addPartContributionToDataVolume(res.part);

    LOG_TRACE(log, "Finished loading {} part {} on disk {}", magic_enum::enum_name(to_state), part_name, part_disk_ptr->getName());
    return res;
}

std::vector<MergeTreeData::LoadPartResult> MergeTreeData::loadDataPartsFromDisk(
    ThreadPool & pool,
    size_t num_parts,
    std::queue<PartLoadingTreeNodes> & parts_queue,
    const MergeTreeSettingsPtr & settings)
{
    /// Parallel loading of data parts.
    pool.setMaxThreads(std::min(static_cast<size_t>(settings->max_part_loading_threads), num_parts));
    size_t num_threads = pool.getMaxThreads();
    LOG_DEBUG(log, "Going to use {} threads to load parts", num_threads);

    std::vector<size_t> parts_per_thread(num_threads, num_parts / num_threads);
    for (size_t i = 0ul; i < num_parts % num_threads; ++i)
        ++parts_per_thread[i];

    /// Prepare data parts for parallel loading. Threads will focus on given disk first, then steal
    /// others' tasks when finish current disk part loading process.
    std::vector<PartLoadingTreeNodes> threads_parts(num_threads);
    std::set<size_t> remaining_thread_parts;
    std::queue<size_t> threads_queue;

    for (size_t i = 0; i < num_threads; ++i)
    {
        remaining_thread_parts.insert(i);
        threads_queue.push(i);
    }

    while (!parts_queue.empty())
    {
        assert(!threads_queue.empty());
        size_t i = threads_queue.front();
        auto & need_parts = parts_per_thread[i];
        assert(need_parts > 0);

        auto & thread_parts = threads_parts[i];
        auto & current_parts = parts_queue.front();
        assert(!current_parts.empty());

        auto parts_to_grab = std::min(need_parts, current_parts.size());
        thread_parts.insert(thread_parts.end(), current_parts.end() - parts_to_grab, current_parts.end());
        current_parts.resize(current_parts.size() - parts_to_grab);
        need_parts -= parts_to_grab;

        /// Before processing next thread, change disk if possible.
        /// Different threads will likely start loading parts from different disk,
        /// which may improve read parallelism for JBOD.

        /// If current disk still has some parts, push it to the tail.
        if (!current_parts.empty())
            parts_queue.push(std::move(current_parts));

        parts_queue.pop();

        /// If current thread still want some parts, push it to the tail.
        if (need_parts > 0)
            threads_queue.push(i);

        threads_queue.pop();
    }

    assert(threads_queue.empty());
    assert(std::all_of(threads_parts.begin(), threads_parts.end(), [](const auto & parts)
    {
        return !parts.empty();
    }));

    std::mutex part_select_mutex;
    std::mutex part_loading_mutex;

    std::vector<LoadPartResult> loaded_parts;

    try
    {
        for (size_t thread = 0; thread < num_threads; ++thread)
        {
            pool.scheduleOrThrowOnError([&, thread, thread_group = CurrentThread::getGroup()]
            {
                SCOPE_EXIT_SAFE(
                    if (thread_group)
                        CurrentThread::detachFromGroupIfNotDetached();
                );
                if (thread_group)
                    CurrentThread::attachToGroupIfDetached(thread_group);

                while (true)
                {
                    PartLoadingTree::NodePtr thread_part;
                    size_t thread_idx = thread;

                    {
                        std::lock_guard lock{part_select_mutex};

                        if (remaining_thread_parts.empty())
                            return;

                        /// Steal task if nothing to do
                        if (threads_parts[thread].empty())
                        {
                            // Try random steal tasks from the next thread
                            std::uniform_int_distribution<size_t> distribution(0, remaining_thread_parts.size() - 1);
                            auto it = remaining_thread_parts.begin();
                            std::advance(it, distribution(thread_local_rng));
                            thread_idx = *it;
                        }

                        auto & thread_parts = threads_parts[thread_idx];
                        thread_part = thread_parts.back();
                        thread_parts.pop_back();
                        if (thread_parts.empty())
                            remaining_thread_parts.erase(thread_idx);
                    }

                    /// Pass a separate mutex to guard the set of parts, because this lambda
                    /// is called concurrently but with already locked @data_parts_mutex.
                    auto res = loadDataPart(thread_part->info, thread_part->name, thread_part->disk, DataPartState::Active, part_loading_mutex);
                    thread_part->is_loaded = true;

                    bool is_active_part = res.part->getState() == DataPartState::Active;
                    /// If part is broken or duplicate or should be removed according to transaction
                    /// and it has any covered parts then try to load them to replace this part.
                    if (!is_active_part && !thread_part->children.empty())
                    {
                        std::lock_guard lock{part_select_mutex};
                        for (const auto & [_, node] : thread_part->children)
                            threads_parts[thread].push_back(node);
                        remaining_thread_parts.insert(thread);
                    }

                    {
                        std::lock_guard lock(part_loading_mutex);
                        loaded_parts.push_back(std::move(res));
                    }
                }
            });
        }
    }
    catch (...)
    {
        /// If this is not done, then in case of an exception, tasks will be destroyed before the threads are completed, and it will be bad.
        pool.wait();
        throw;
    }

    pool.wait();
    return loaded_parts;
}


void MergeTreeData::loadDataPartsFromWAL(MutableDataPartsVector & parts_from_wal)
{
    std::sort(parts_from_wal.begin(), parts_from_wal.end(), [](const auto & lhs, const auto & rhs)
    {
        return std::tie(lhs->info.level, lhs->info.mutation) > std::tie(rhs->info.level, rhs->info.mutation);
    });

    for (auto & part : parts_from_wal)
    {
        part->modification_time = time(nullptr);
        auto lo = data_parts_by_state_and_info.lower_bound(DataPartStateAndInfo{DataPartState::Active, part->info});

        if (lo != data_parts_by_state_and_info.begin() && (*std::prev(lo))->info.contains(part->info))
            continue;

        if (lo != data_parts_by_state_and_info.end() && (*lo)->info.contains(part->info))
            continue;

        part->setState(DataPartState::Active);
        auto [it, inserted] = data_parts_indexes.insert(part);

        if (!inserted)
        {
            if ((*it)->checksums.getTotalChecksumHex() == part->checksums.getTotalChecksumHex())
                LOG_ERROR(log, "Remove duplicate part {}", part->getDataPartStoragePtr()->getFullPath());
            else
                throw Exception(ErrorCodes::DUPLICATE_DATA_PART, "Part {} already exists but with different checksums", part->name);
        }
        else
        {
            addPartContributionToDataVolume(part);
        }
    }
}


void MergeTreeData::loadDataParts(bool skip_sanity_checks)
{
    LOG_DEBUG(log, "Loading data parts");

    auto metadata_snapshot = getInMemoryMetadataPtr();
    const auto settings = getSettings();
    Strings part_file_names;

    auto disks = getStoragePolicy()->getDisks();

    /// Only check if user did touch storage configuration for this table.
    if (!getStoragePolicy()->isDefaultPolicy() && !skip_sanity_checks)
    {
        /// Check extra parts at different disks, in order to not allow to miss data parts at undefined disks.
        std::unordered_set<String> defined_disk_names;

        for (const auto & disk_ptr : disks)
        {
            defined_disk_names.insert(disk_ptr->getName());
        }

        /// In case of delegate disks it is not enough to traverse `disks`,
        /// because for example cache or encrypted disk which wrap s3 disk and s3 disk itself can be put into different storage policies.
        /// But disk->exists returns the same thing for both disks.
        for (const auto & [disk_name, disk] : getContext()->getDisksMap())
        {
            /// As encrypted disk can use the same path of its nested disk,
            /// we need to take it into account here.
            const auto & delegate = disk->getDelegateDiskIfExists();
            if (delegate && disk->getPath() == delegate->getPath())
                defined_disk_names.insert(delegate->getName());

            if (disk->supportsCache())
            {
                /// As cache is implemented on object storage layer, not on disk level, e.g.
                /// we have such structure:
                /// DiskObjectStorage(CachedObjectStorage(...(CachedObjectStored(ObjectStorage)...)))
                /// and disk_ptr->getName() here is the name of last delegate - ObjectStorage.
                /// So now we need to add cache layers to defined disk names.
                auto caches = disk->getCacheLayersNames();
                defined_disk_names.insert(caches.begin(), caches.end());
            }
        }

        for (const auto & [disk_name, disk] : getContext()->getDisksMap())
        {
            if (disk->isBroken() || disk->isCustomDisk())
                continue;

            if (!defined_disk_names.contains(disk_name) && disk->exists(relative_data_path))
            {
                for (const auto it = disk->iterateDirectory(relative_data_path); it->isValid(); it->next())
                {
                    if (MergeTreePartInfo::tryParsePartName(it->name(), format_version))
                    {
                        throw Exception(
                            ErrorCodes::UNKNOWN_DISK,
                            "Part {} ({}) was found on disk {} which is not defined in the storage policy (defined disks: {})",
                            backQuote(it->name()), backQuote(it->path()), backQuote(disk_name), fmt::join(defined_disk_names, ", "));
                    }
                }
            }
        }
    }

    ThreadPool pool(CurrentMetrics::MergeTreePartsLoaderThreads, CurrentMetrics::MergeTreePartsLoaderThreadsActive, disks.size());
    std::vector<PartLoadingTree::PartLoadingInfos> parts_to_load_by_disk(disks.size());

    for (size_t i = 0; i < disks.size(); ++i)
    {
        const auto & disk_ptr = disks[i];
        if (disk_ptr->isBroken())
            continue;

        auto & disk_parts = parts_to_load_by_disk[i];

        pool.scheduleOrThrowOnError([&, disk_ptr]()
        {
            for (auto it = disk_ptr->iterateDirectory(relative_data_path); it->isValid(); it->next())
            {
                /// Skip temporary directories, file 'format_version.txt' and directory 'detached'.
                if (startsWith(it->name(), "tmp") || it->name() == MergeTreeData::FORMAT_VERSION_FILE_NAME
                    || it->name() == MergeTreeData::DETACHED_DIR_NAME
                    || startsWith(it->name(), MergeTreeWriteAheadLog::WAL_FILE_NAME))
                    continue;

                if (auto part_info = MergeTreePartInfo::tryParsePartName(it->name(), format_version))
                    disk_parts.emplace_back(*part_info, it->name(), disk_ptr);
            }
        });
    }

    pool.wait();

    PartLoadingTree::PartLoadingInfos parts_to_load;
    for (auto & disk_parts : parts_to_load_by_disk)
        std::move(disk_parts.begin(), disk_parts.end(), std::back_inserter(parts_to_load));

    auto loading_tree = PartLoadingTree::build(std::move(parts_to_load));
    /// Collect parts by disks' names.
    std::map<String, PartLoadingTreeNodes> disk_part_map;

    /// Collect only "the most covering" parts from the top level of the tree.
    loading_tree.traverse(/*recursive=*/ false, [&](const auto & node)
    {
        disk_part_map[node->disk->getName()].emplace_back(node);
    });

    size_t num_parts = 0;
    std::queue<PartLoadingTreeNodes> parts_queue;

    for (auto & [disk_name, disk_parts] : disk_part_map)
    {
        LOG_INFO(log, "Found {} parts for disk '{}' to load", disk_parts.size(), disk_name);

        if (disk_parts.empty())
            continue;

        num_parts += disk_parts.size();
        parts_queue.push(std::move(disk_parts));
    }

    auto part_lock = lockParts();
    data_parts_indexes.clear();

    MutableDataPartsVector broken_parts_to_detach;
    MutableDataPartsVector duplicate_parts_to_remove;

    size_t suspicious_broken_parts = 0;
    size_t suspicious_broken_parts_bytes = 0;
    bool have_adaptive_parts = false;
    bool have_non_adaptive_parts = false;
    bool have_lightweight_in_parts = false;
    bool have_parts_with_version_metadata = false;

    bool is_static_storage = isStaticStorage();

    if (num_parts > 0)
    {
        auto loaded_parts = loadDataPartsFromDisk(pool, num_parts, parts_queue, settings);

        for (const auto & res : loaded_parts)
        {
            if (res.is_broken)
            {
                broken_parts_to_detach.push_back(res.part);
                ++suspicious_broken_parts;
                if (res.size_of_part)
                    suspicious_broken_parts_bytes += *res.size_of_part;
            }
            else if (res.part->is_duplicate)
            {
                if (!is_static_storage)
                    res.part->remove();
            }
            else
            {
                bool is_adaptive = res.part->index_granularity_info.mark_type.adaptive;
                have_adaptive_parts |= is_adaptive;
                have_non_adaptive_parts |= !is_adaptive;
                have_lightweight_in_parts |= res.part->hasLightweightDelete();
                have_parts_with_version_metadata |= res.part->wasInvolvedInTransaction();
            }
        }
    }

    if (settings->in_memory_parts_enable_wal)
    {
        pool.setMaxThreads(disks.size());
        std::vector<MutableDataPartsVector> disks_wal_parts(disks.size());
        std::mutex wal_init_lock;

        for (size_t i = 0; i < disks.size(); ++i)
        {
            const auto & disk_ptr = disks[i];
            if (disk_ptr->isBroken())
                continue;

            auto & disk_wal_parts = disks_wal_parts[i];

            pool.scheduleOrThrowOnError([&, disk_ptr]()
            {
                for (auto it = disk_ptr->iterateDirectory(relative_data_path); it->isValid(); it->next())
                {
                    if (!startsWith(it->name(), MergeTreeWriteAheadLog::WAL_FILE_NAME))
                        continue;

                    if (it->name() == MergeTreeWriteAheadLog::DEFAULT_WAL_FILE_NAME)
                    {
                        std::lock_guard lock(wal_init_lock);
                        if (write_ahead_log != nullptr)
                            throw Exception(ErrorCodes::CORRUPTED_DATA,
                                            "There are multiple WAL files appeared in current storage policy. "
                                            "You need to resolve this manually");

                        write_ahead_log = std::make_shared<MergeTreeWriteAheadLog>(*this, disk_ptr, it->name());
                        for (auto && part : write_ahead_log->restore(metadata_snapshot, getContext(), part_lock))
                            disk_wal_parts.push_back(std::move(part));
                    }
                    else
                    {
                        MergeTreeWriteAheadLog wal(*this, disk_ptr, it->name());
                        for (auto && part : wal.restore(metadata_snapshot, getContext(), part_lock))
                            disk_wal_parts.push_back(std::move(part));
                    }
                }
            });
        }

        pool.wait();

        MutableDataPartsVector parts_from_wal;
        for (auto & disk_wal_parts : disks_wal_parts)
            std::move(disk_wal_parts.begin(), disk_wal_parts.end(), std::back_inserter(parts_from_wal));

        loadDataPartsFromWAL(parts_from_wal);
        num_parts += parts_from_wal.size();
    }

    if (num_parts == 0)
    {
        /// resetObjectColumnsFromActiveParts(part_lock);
        LOG_DEBUG(log, "There are no data parts");
        return;
    }

    if (have_non_adaptive_parts && have_adaptive_parts && !settings->enable_mixed_granularity_parts)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Table contains parts with adaptive and non adaptive marks, "
                        "but `setting enable_mixed_granularity_parts` is disabled");

    has_non_adaptive_index_granularity_parts = have_non_adaptive_parts;
    has_lightweight_delete_parts = have_lightweight_in_parts;
    transactions_enabled = have_parts_with_version_metadata;

    if (suspicious_broken_parts > settings->max_suspicious_broken_parts && !skip_sanity_checks)
        throw Exception(ErrorCodes::TOO_MANY_UNEXPECTED_DATA_PARTS,
            "Suspiciously many ({} parts, {} in total) broken parts to remove while maximum allowed broken parts count is {}. You can change the maximum value "
                        "with merge tree setting 'max_suspicious_broken_parts' in <merge_tree> configuration section or in table settings in .sql file "
                        "(don't forget to return setting back to default value)",
            suspicious_broken_parts, formatReadableSizeWithBinarySuffix(suspicious_broken_parts_bytes), settings->max_suspicious_broken_parts);

    if (suspicious_broken_parts_bytes > settings->max_suspicious_broken_parts_bytes && !skip_sanity_checks)
        throw Exception(ErrorCodes::TOO_MANY_UNEXPECTED_DATA_PARTS,
            "Suspiciously big size ({} parts, {} in total) of all broken parts to remove while maximum allowed broken parts size is {}. "
            "You can change the maximum value with merge tree setting 'max_suspicious_broken_parts_bytes' in <merge_tree> configuration "
            "section or in table settings in .sql file (don't forget to return setting back to default value)",
            suspicious_broken_parts, formatReadableSizeWithBinarySuffix(suspicious_broken_parts_bytes),
            formatReadableSizeWithBinarySuffix(settings->max_suspicious_broken_parts_bytes));

    if (!is_static_storage)
        for (auto & part : broken_parts_to_detach)
            part->renameToDetached("broken-on-start"); /// detached parts must not have '_' in prefixes

    /// resetObjectColumnsFromActiveParts(part_lock);
    calculateColumnAndSecondaryIndexSizesImpl();

    PartLoadingTreeNodes unloaded_parts;
    loading_tree.traverse(/*recursive=*/ true, [&](const auto & node)
    {
        if (!node->is_loaded)
            unloaded_parts.push_back(node);
    });

    if (!unloaded_parts.empty())
    {
        LOG_DEBUG(log, "Found {} outdated data parts. They will be loaded asynchronously", unloaded_parts.size());

        {
            std::lock_guard lock(outdated_data_parts_mutex);
            outdated_unloaded_data_parts = std::move(unloaded_parts);
        }

        outdated_data_parts_loading_task = getContext()->getSchedulePool().createTask(
            "MergeTreeData::loadOutdatedDataParts",
            [this] { loadOutdatedDataParts(/*is_async=*/ true); });
    }

    LOG_DEBUG(log, "Loaded data parts ({} items)", data_parts_indexes.size());
    data_parts_loading_finished = true;
}

void MergeTreeData::loadOutdatedDataParts(bool is_async)
try
{
    {
        std::lock_guard lock(outdated_data_parts_mutex);
        if (outdated_unloaded_data_parts.empty())
            return;

        LOG_DEBUG(log, "Loading {} outdated data parts {}",
            outdated_unloaded_data_parts.size(),
            is_async ? "asynchronously" : "synchronously");
    }

    /// Acquire shared lock because 'relative_data_path' is used while loading parts.
    TableLockHolder shared_lock;
    if (is_async)
        shared_lock = lockForShare(RWLockImpl::NO_QUERY, getSettings()->lock_acquire_timeout_for_background_operations);

    size_t num_loaded_parts = 0;
    while (true)
    {
        PartLoadingTree::NodePtr part;

        {
            std::lock_guard lock(outdated_data_parts_mutex);

            if (is_async && outdated_data_parts_loading_canceled)
            {
                LOG_DEBUG(log,
                    "Stopped loading outdated data parts because task was canceled. "
                    "Loaded {} parts, {} left unloaded", num_loaded_parts, outdated_unloaded_data_parts.size());
                return;
            }

            if (outdated_unloaded_data_parts.empty())
                break;

            part = std::move(outdated_unloaded_data_parts.back());
            outdated_unloaded_data_parts.pop_back();
        }

        auto res = loadDataPart(part->info, part->name, part->disk, MergeTreeDataPartState::Outdated, data_parts_mutex);
        ++num_loaded_parts;

        if (res.is_broken)
            res.part->renameToDetached("broken-on-start"); /// detached parts must not have '_' in prefixes
        else if (res.part->is_duplicate)
            res.part->remove();
        else
            preparePartForRemoval(res.part);
    }

    LOG_DEBUG(log, "Loaded {} outdated data parts {}",
        num_loaded_parts, is_async ? "asynchronously" : "synchronously");
    outdated_data_parts_cv.notify_all();
}
catch (...)
{
    LOG_ERROR(log, "Loading of outdated parts failed. "
        "Will terminate to avoid undefined behaviour due to inconsistent set of parts. "
        "Exception: {}", getCurrentExceptionMessage(true));
    std::terminate();
}

/// No TSA because of std::unique_lock and std::condition_variable.
void MergeTreeData::waitForOutdatedPartsToBeLoaded() const TSA_NO_THREAD_SAFETY_ANALYSIS
{
    /// Background tasks are not run if storage is static.
    if (isStaticStorage())
        return;

    std::unique_lock lock(outdated_data_parts_mutex);
    if (outdated_unloaded_data_parts.empty())
        return;

    LOG_TRACE(log, "Will wait for outdated data parts to be loaded");

    outdated_data_parts_cv.wait(lock, [this]() TSA_NO_THREAD_SAFETY_ANALYSIS
    {
        return outdated_unloaded_data_parts.empty() || outdated_data_parts_loading_canceled;
    });

    if (outdated_data_parts_loading_canceled)
        throw Exception(ErrorCodes::NOT_INITIALIZED, "Loading of outdated data parts was canceled");

    LOG_TRACE(log, "Finished waiting for outdated data parts to be loaded");
}

void MergeTreeData::startOutdatedDataPartsLoadingTask()
{
    if (outdated_data_parts_loading_task)
        outdated_data_parts_loading_task->activateAndSchedule();
}

void MergeTreeData::stopOutdatedDataPartsLoadingTask()
{
    if (!outdated_data_parts_loading_task)
        return;

    {
        std::lock_guard lock(outdated_data_parts_mutex);
        outdated_data_parts_loading_canceled = true;
    }

    outdated_data_parts_loading_task->deactivate();
    outdated_data_parts_cv.notify_all();
}

/// Is the part directory old.
/// True if its modification time and the modification time of all files inside it is less then threshold.
/// (Only files on the first level of nesting are considered).
static bool isOldPartDirectory(const DiskPtr & disk, const String & directory_path, time_t threshold)
{
    if (!disk->isDirectory(directory_path) || disk->getLastModified(directory_path).epochTime() >= threshold)
        return false;

    for (auto it = disk->iterateDirectory(directory_path); it->isValid(); it->next())
        if (disk->getLastModified(it->path()).epochTime() >= threshold)
            return false;

    return true;
}


size_t MergeTreeData::clearOldTemporaryDirectories(size_t custom_directories_lifetime_seconds, const NameSet & valid_prefixes)
{
    /// If the method is already called from another thread, then we don't need to do anything.
    std::unique_lock lock(clear_old_temporary_directories_mutex, std::defer_lock);
    if (!lock.try_lock())
        return 0;

    const auto settings = getSettings();
    time_t current_time = time(nullptr);
    ssize_t deadline = current_time - custom_directories_lifetime_seconds;

    size_t cleared_count = 0;

    /// Delete temporary directories older than a day.
    for (const auto & disk : getDisks())
    {
        if (disk->isBroken())
            continue;

        for (auto it = disk->iterateDirectory(relative_data_path); it->isValid(); it->next())
        {
            const std::string & basename = it->name();
            bool start_with_valid_prefix = false;
            for (const auto & prefix : valid_prefixes)
            {
                if (startsWith(basename, prefix))
                {
                    start_with_valid_prefix = true;
                    break;
                }
            }

            if (!start_with_valid_prefix)
                continue;

            const std::string & full_path = fullPath(disk, it->path());

            try
            {
                if (isOldPartDirectory(disk, it->path(), deadline))
                {
                    if (temporary_parts.contains(basename))
                    {
                        /// Actually we don't rely on temporary_directories_lifetime when removing old temporaries directories,
                        /// it's just an extra level of protection just in case we have a bug.
                        LOG_INFO(LogFrequencyLimiter(log.load(), 10), "{} is in use (by merge/mutation/INSERT) (consider increasing temporary_directories_lifetime setting)", full_path);
                        continue;
                    }
                    else if (!disk->exists(it->path()))
                    {
                        /// We should recheck that the dir exists, otherwise we can get "No such file or directory"
                        /// due to a race condition with "Renaming temporary part" (temporary part holder could be already released, so the check above is not enough)
                        LOG_WARNING(log, "Temporary directory {} suddenly disappeared while iterating, assuming it was concurrently renamed to persistent", it->path());
                        continue;
                    }
                    else
                    {
                        LOG_WARNING(log, "Removing temporary directory {}", full_path);

                        /// Even if it's a temporary part it could be downloaded with zero copy replication and this function
                        /// is executed as a callback.
                        ///
                        /// We don't control the amount of refs for temporary parts so we cannot decide can we remove blobs
                        /// or not. So we are not doing it
                        bool keep_shared = false;
                        if (disk->supportZeroCopyReplication() && settings->allow_remote_fs_zero_copy_replication)
                        {
                            LOG_WARNING(log, "Since zero-copy replication is enabled we are not going to remove blobs from shared storage for {}", full_path);
                            keep_shared = true;
                        }

                        disk->removeSharedRecursive(it->path(), keep_shared, {});
                        ++cleared_count;
                    }
                }
            }
            catch (const fs::filesystem_error & e)
            {
                if (e.code() == std::errc::no_such_file_or_directory)
                {
                    /// If the file is already deleted, do nothing.
                }
                else
                    throw;
            }
        }
    }

    return cleared_count;
}

scope_guard MergeTreeData::getTemporaryPartDirectoryHolder(const String & part_dir_name) const
{
    temporary_parts.add(part_dir_name);
    return [this, part_dir_name]() { temporary_parts.remove(part_dir_name); };
}

/// proton: starts
UInt64 MergeTreeData::getStorageSize() const
{
    DataPartStateVector all_parts_state;

    const auto & all_parts = getAllDataPartsVector(&all_parts_state);

    UInt64 total_size = 0;

    {
        auto lock = lockParts();
        for (const auto & part : all_parts)
            total_size += part->getBytesOnDisk();
    }

    return total_size;
}
/// proton: ends

MergeTreeData::MutableDataPartPtr MergeTreeData::asMutableDeletingPart(const DataPartPtr & part)
{
    auto state = part->getState();
    if (state != DataPartState::Deleting && state != DataPartState::DeleteOnDestroy)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Cannot remove part {}, because it has state: {}", part->name, magic_enum::enum_name(state));

    return std::const_pointer_cast<IMergeTreeDataPart>(part);
}

MergeTreeData::DataPartsVector MergeTreeData::grabOldParts(bool force)
{
    DataPartsVector res;

    /// If the method is already called from another thread, then we don't need to do anything.
    std::unique_lock lock(grab_old_parts_mutex, std::defer_lock);
    if (!lock.try_lock())
        return res;

    bool need_remove_parts_in_order = supportsReplication() && getSettings()->allow_remote_fs_zero_copy_replication;
    if (need_remove_parts_in_order)
    {
        bool has_zero_copy_disk = false;
        for (const auto & disk : getDisks())
        {
            if (disk->supportZeroCopyReplication())
            {
                has_zero_copy_disk = true;
                break;
            }
        }
        need_remove_parts_in_order = has_zero_copy_disk;
    }

    std::vector<DataPartIteratorByStateAndInfo> parts_to_delete;
    std::vector<MergeTreePartInfo> skipped_parts;

    auto has_skipped_mutation_parent = [&skipped_parts, need_remove_parts_in_order] (const DataPartPtr & part)
    {
        if (!need_remove_parts_in_order)
            return false;

        for (const auto & part_info : skipped_parts)
            if (part->info.isMutationChildOf(part_info))
                return true;

        return false;
    };

    auto time_now = time(nullptr);

    {
        auto parts_lock = lockParts();

        auto outdated_parts_range = getDataPartsStateRange(DataPartState::Outdated);
        for (auto it = outdated_parts_range.begin(); it != outdated_parts_range.end(); ++it)
        {
            const DataPartPtr & part = *it;

            part->last_removal_attemp_time.store(time_now, std::memory_order_relaxed);

            /// Do not remove outdated part if it may be visible for some transaction
            if (!part->version.canBeRemoved())
            {
                part->removal_state.store(DataPartRemovalState::VISIBLE_TO_TRANSACTIONS, std::memory_order_relaxed);
                skipped_parts.push_back(part->info);
                continue;
            }

            /// Grab only parts that are not used by anyone (SELECTs for example).
            if (!isSharedPtrUnique(part))
            {
                part->removal_state.store(DataPartRemovalState::NON_UNIQUE_OWNERSHIP, std::memory_order_relaxed);
                skipped_parts.push_back(part->info);
                continue;
            }

            auto part_remove_time = part->remove_time.load(std::memory_order_relaxed);
            bool reached_removal_time = part_remove_time < time_now && time_now - part_remove_time > getSettings()->old_parts_lifetime.totalSeconds();
            if ((reached_removal_time && !has_skipped_mutation_parent(part))
                || force
                || isInMemoryPart(part)     /// Remove in-memory parts immediately to not store excessive data in RAM
                || (part->version.creation_csn == Tx::RolledBackCSN && getSettings()->remove_rolled_back_parts_immediately))
            {
                part->removal_state.store(DataPartRemovalState::REMOVED, std::memory_order_relaxed);
                parts_to_delete.emplace_back(it);
            }
            else
            {
                if (!reached_removal_time)
                    part->removal_state.store(DataPartRemovalState::NOT_REACHED_REMOVAL_TIME, std::memory_order_relaxed);
                else
                    part->removal_state.store(DataPartRemovalState::HAS_SKIPPED_MUTATION_PARENT, std::memory_order_relaxed);
                skipped_parts.push_back(part->info);
                continue;
            }
        }

        res.reserve(parts_to_delete.size());
        for (const auto & it_to_delete : parts_to_delete)
        {
            res.emplace_back(*it_to_delete);
            modifyPartState(it_to_delete, DataPartState::Deleting);
        }
    }

    if (!res.empty())
        LOG_TRACE(log, "Found {} old parts to remove. Parts: [{}]",
                  res.size(), fmt::join(getPartsNames(res), ", "));

    return res;
}


void MergeTreeData::rollbackDeletingParts(const MergeTreeData::DataPartsVector & parts)
{
    auto lock = lockParts();
    for (const auto & part : parts)
    {
        /// We should modify it under data_parts_mutex
        part->assertState({DataPartState::Deleting});
        modifyPartState(part, DataPartState::Outdated);
    }
}

void MergeTreeData::removePartsFinally(const MergeTreeData::DataPartsVector & parts)
{
    if (parts.empty())
        return;

    {
        auto lock = lockParts();

        /// TODO: use data_parts iterators instead of pointers
        for (const auto & part : parts)
        {
            /// Temporary does not present in data_parts_by_info.
            if (part->getState() == DataPartState::Temporary)
                continue;

            auto it = data_parts_by_info.find(part->info);
            if (it == data_parts_by_info.end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Deleting data part {} doesn't exist", part->name);

            (*it)->assertState({DataPartState::Deleting});

            data_parts_indexes.erase(it);
        }
    }

    LOG_DEBUG(log, "Removing {} parts from memory: Parts: [{}]", parts.size(), fmt::join(parts, ", "));

    /// Data parts is still alive (since DataPartsVector holds shared_ptrs) and contain useful metainformation for logging
    /// NOTE: There is no need to log parts deletion somewhere else, all deleting parts pass through this function and pass away

    auto table_id = getStorageID();
    if (auto part_log = getContext()->getPartLog(table_id.database_name))
    {
        PartLogElement part_log_elem;

        part_log_elem.event_type = PartLogElement::REMOVE_PART;

        const auto time_now = std::chrono::system_clock::now();
        part_log_elem.event_time = timeInSeconds(time_now);
        part_log_elem.event_time_microseconds = timeInMicroseconds(time_now);

        part_log_elem.duration_ms = 0; //-V1048

        part_log_elem.database_name = table_id.database_name;
        part_log_elem.table_name = table_id.table_name;

        for (const auto & part : parts)
        {
            part_log_elem.partition_id = part->info.partition_id;
            part_log_elem.partition = part->partition.serializeToString(part->getMetadataSnapshot());
            part_log_elem.part_name = part->name;
            part_log_elem.bytes_compressed_on_disk = part->getBytesOnDisk();
            part_log_elem.rows = part->rows_count;
            part_log_elem.part_type = part->getType();

            part_log->add(part_log_elem);
        }
    }
}

void MergeTreeData::flushAllInMemoryPartsIfNeeded()
{
    if (getSettings()->in_memory_parts_enable_wal)
        return;

    auto metadata_snapshot = getInMemoryMetadataPtr();
    DataPartsVector parts = getDataPartsVectorForInternalUsage();
    for (const auto & part : parts)
    {
        if (auto part_in_memory = asInMemoryPart(part))
        {
            part_in_memory->flushToDisk(part_in_memory->getDataPartStorage().getPartDirectory(), metadata_snapshot);
        }
    }
}

size_t MergeTreeData::clearOldPartsFromFilesystem(bool force)
{
    DataPartsVector parts_to_remove = grabOldParts(force);
    if (parts_to_remove.empty())
        return 0;

    clearPartsFromFilesystem(parts_to_remove);
    removePartsFinally(parts_to_remove);
    /// This is needed to close files to avoid they reside on disk after being deleted.
    /// NOTE: we can drop files from cache more selectively but this is good enough.
    getContext()->dropMMappedFileCache();

    return parts_to_remove.size();
}


void MergeTreeData::clearPartsFromFilesystem(const DataPartsVector & parts, bool throw_on_error, NameSet * parts_failed_to_delete)
{
    NameSet part_names_succeed;

    auto get_failed_parts = [&part_names_succeed, &parts_failed_to_delete, &parts] ()
    {
        if (part_names_succeed.size() == parts.size())
            return;

        if (parts_failed_to_delete)
        {
            for (const auto & part : parts)
            {
                if (!part_names_succeed.contains(part->name))
                    parts_failed_to_delete->insert(part->name);
            }
        }
    };

    try
    {
        clearPartsFromFilesystemImpl(parts, &part_names_succeed);
        get_failed_parts();
    }
    catch (...)
    {
        get_failed_parts();

        LOG_DEBUG(log, "Failed to remove all parts, all count {}, removed {}", parts.size(), part_names_succeed.size());

        if (throw_on_error)
            throw;
    }
}

void MergeTreeData::clearPartsFromFilesystemImpl(const DataPartsVector & parts_to_remove, NameSet * part_names_succeed)
{
    if (parts_to_remove.empty())
        return;

    const auto settings = getSettings();

    auto remove_single_thread = [this, &parts_to_remove, part_names_succeed]()
    {
        LOG_DEBUG(
            log, "Removing {} parts from filesystem (serially): Parts: [{}]", parts_to_remove.size(), fmt::join(parts_to_remove, ", "));
        for (const DataPartPtr & part : parts_to_remove)
        {
            asMutableDeletingPart(part)->remove();
            if (part_names_succeed)
                part_names_succeed->insert(part->name);
        }
    };

    if (settings->max_part_removal_threads <= 1 || parts_to_remove.size() <= settings->concurrent_part_removal_threshold)
    {
        remove_single_thread();
        return;
    }

    /// Parallel parts removal.
    size_t num_threads = std::min<size_t>(settings->max_part_removal_threads, parts_to_remove.size());
    std::mutex part_names_mutex;
    ThreadPool pool(CurrentMetrics::MergeTreePartsCleanerThreads, CurrentMetrics::MergeTreePartsCleanerThreadsActive, num_threads);

    bool has_zero_copy_parts = false;
    /// proton: starts
    /// we do not support replicatedMergeTree yet, so we can't use it
    /// clickhouse make the settings->allow_remote_fs_zero_copy_replication to false by default
    /// https://github.com/ClickHouse/ClickHouse/pull/40175/files#diff-c8c4e0e14aea78429a8b1d7da90ae5e98bd2c440bf7c03c3463b5234612c99f1R143
    // if (settings->allow_remote_fs_zero_copy_replication && dynamic_cast<StorageReplicatedMergeTree *>(this) != nullptr)
    // {
    //     has_zero_copy_parts = std::any_of(
    //         parts_to_remove.begin(), parts_to_remove.end(),
    //         [] (const auto & data_part) { return data_part->isStoredOnRemoteDiskWithZeroCopySupport(); }
    //     );
    // }
    /// proton: ends

    if (!has_zero_copy_parts)
    {
        /// NOTE: Under heavy system load you may get "Cannot schedule a task" from ThreadPool.
        LOG_DEBUG(
            log, "Removing {} parts from filesystem (concurrently): Parts: [{}]", parts_to_remove.size(), fmt::join(parts_to_remove, ", "));

        for (const DataPartPtr & part : parts_to_remove)
        {
            pool.scheduleOrThrowOnError([&part, &part_names_mutex, part_names_succeed, thread_group = CurrentThread::getGroup()]
            {
                SCOPE_EXIT_SAFE(
                    if (thread_group)
                        CurrentThread::detachFromGroupIfNotDetached();
                );
                if (thread_group)
                    CurrentThread::attachToGroupIfDetached(thread_group);

                asMutableDeletingPart(part)->remove();
                if (part_names_succeed)
                {
                    std::lock_guard lock(part_names_mutex);
                    part_names_succeed->insert(part->name);
                }
            });
        }

        pool.wait();
        return;
    }

    if (format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
    {
        remove_single_thread();
        return;
    }

    /// NOTE: Under heavy system load you may get "Cannot schedule a task" from ThreadPool.
    LOG_DEBUG(
        log, "Removing {} parts from filesystem (concurrently): Parts: [{}]", parts_to_remove.size(), fmt::join(parts_to_remove, ", "));

    /// We have "zero copy replication" parts and we are going to remove them in parallel.
    /// The problem is that all parts in a mutation chain must be removed sequentially to avoid "key does not exits" issues.
    /// We remove disjoint subsets of parts in parallel.
    /// The problem is that it's not trivial to divide Outdated parts into disjoint subsets,
    /// because Outdated parts legally can be intersecting (but intersecting parts must be separated by a DROP_RANGE).
    /// So we ignore level and version and use block numbers only.
    ActiveDataPartSet independent_ranges_set(format_version);
    for (const auto & part : parts_to_remove)
    {
        MergeTreePartInfo range_info = part->info;
        range_info.level = static_cast<UInt32>(range_info.max_block - range_info.min_block);
        range_info.mutation = 0;
        independent_ranges_set.add(range_info, range_info.getPartNameV1());
    }

    auto independent_ranges_infos = independent_ranges_set.getPartInfos();
    size_t sum_of_ranges = 0;
    for (auto range : independent_ranges_infos)
    {
        range.level = MergeTreePartInfo::MAX_LEVEL;
        range.mutation = MergeTreePartInfo::MAX_BLOCK_NUMBER;

        DataPartsVector parts_in_range;
        for (const auto & part : parts_to_remove)
            if (range.contains(part->info))
                parts_in_range.push_back(part);
        sum_of_ranges += parts_in_range.size();

        pool.scheduleOrThrowOnError(
            [this, range, &part_names_mutex, part_names_succeed, thread_group = CurrentThread::getGroup(), batch = std::move(parts_in_range)]
        {
            SCOPE_EXIT_SAFE(
                if (thread_group)
                    CurrentThread::detachFromGroupIfNotDetached();
            );
            if (thread_group)
                CurrentThread::attachToGroupIfDetached(thread_group);

            LOG_TRACE(log, "Removing {} parts in blocks range {}", batch.size(), range.getPartNameForLogs());

            for (const auto & part : batch)
            {
                asMutableDeletingPart(part)->remove();
                if (part_names_succeed)
                {
                    std::lock_guard lock(part_names_mutex);
                    part_names_succeed->insert(part->name);
                }
            }
        });
    }

    pool.wait();

    if (parts_to_remove.size() != sum_of_ranges)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Number of removed parts is not equal to number of parts in independent ranges "
                                                   "({} != {}), it's a bug", parts_to_remove.size(), sum_of_ranges);
}

size_t MergeTreeData::clearOldBrokenPartsFromDetachedDirecory()
{
    /**
     * Remove old (configured by setting) broken detached parts.
     * Only parts with certain prefixes are removed. These prefixes
     * are such that it is guaranteed that they will never be needed
     * and need to be cleared. ctime is used to check when file was
     * moved to detached/ directory (see https://unix.stackexchange.com/a/211134)
     */

    DetachedPartsInfo detached_parts = getDetachedParts();
    if (detached_parts.empty())
        return 0;

    PartsTemporaryRename renamed_parts(*this, "detached/");

    for (const auto & part_info : detached_parts)
    {
        if (!part_info.valid_name || part_info.prefix.empty())
            continue;

        const auto & removable_detached_parts_prefixes = DetachedPartInfo::DETACHED_REASONS_REMOVABLE_BY_TIMEOUT;
        bool can_be_removed_by_timeout = std::find(
                                             removable_detached_parts_prefixes.begin(),
                                             removable_detached_parts_prefixes.end(),
                                             part_info.prefix) != removable_detached_parts_prefixes.end();

        if (!can_be_removed_by_timeout)
            continue;

        time_t current_time = time(nullptr);
        ssize_t threshold = current_time - getSettings()->merge_tree_clear_old_broken_detached_parts_ttl_timeout_seconds;
        auto path = fs::path(relative_data_path) / "detached" / part_info.dir_name;
        time_t last_change_time = part_info.disk->getLastChanged(path);
        time_t last_modification_time = part_info.disk->getLastModified(path).epochTime();
        time_t last_touch_time = std::max(last_change_time, last_modification_time);

        if (last_touch_time == 0 || last_touch_time >= threshold)
            continue;

        renamed_parts.addPart(part_info.dir_name, "deleting_" + part_info.dir_name, part_info.disk);
    }

    LOG_INFO(log, "Will clean up {} detached parts", renamed_parts.old_and_new_names.size());

    renamed_parts.tryRenameAll();

    for (const auto & [old_name, new_name, disk] : renamed_parts.old_and_new_names)
    {
        removeDetachedPart(disk, fs::path(relative_data_path) / "detached" / new_name / "", old_name, false);
        LOG_DEBUG(log, "Removed broken detached part {} due to a timeout for broken detached parts", old_name);
    }

    return renamed_parts.old_and_new_names.size();
}

size_t MergeTreeData::clearOldWriteAheadLogs()
{
    DataPartsVector parts = getDataPartsVectorForInternalUsage();
    std::vector<std::pair<Int64, Int64>> all_block_numbers_on_disk;
    std::vector<std::pair<Int64, Int64>> block_numbers_on_disk;

    for (const auto & part : parts)
        if (part->isStoredOnDisk())
            all_block_numbers_on_disk.emplace_back(part->info.min_block, part->info.max_block);

    if (all_block_numbers_on_disk.empty())
        return 0;

    ::sort(all_block_numbers_on_disk.begin(), all_block_numbers_on_disk.end());
    block_numbers_on_disk.push_back(all_block_numbers_on_disk[0]);
    for (size_t i = 1; i < all_block_numbers_on_disk.size(); ++i)
    {
        if (all_block_numbers_on_disk[i].first == all_block_numbers_on_disk[i - 1].second + 1)
            block_numbers_on_disk.back().second = all_block_numbers_on_disk[i].second;
        else
            block_numbers_on_disk.push_back(all_block_numbers_on_disk[i]);
    }

    auto is_range_on_disk = [&block_numbers_on_disk](Int64 min_block, Int64 max_block)
    {
        auto lower = std::lower_bound(block_numbers_on_disk.begin(), block_numbers_on_disk.end(), std::make_pair(min_block, Int64(-1L)));
        if (lower != block_numbers_on_disk.end() && min_block >= lower->first && max_block <= lower->second)
            return true;

        if (lower != block_numbers_on_disk.begin())
        {
            --lower;
            if (min_block >= lower->first && max_block <= lower->second)
                return true;
        }

        return false;
    };

    size_t cleared_count = 0;
    auto disks = getStoragePolicy()->getDisks();
    for (auto disk_it = disks.rbegin(); disk_it != disks.rend(); ++disk_it)
    {
        auto disk_ptr = *disk_it;
        if (disk_ptr->isBroken())
            continue;

        for (auto it = disk_ptr->iterateDirectory(relative_data_path); it->isValid(); it->next())
        {
            auto min_max_block_number = MergeTreeWriteAheadLog::tryParseMinMaxBlockNumber(it->name());
            if (min_max_block_number && is_range_on_disk(min_max_block_number->first, min_max_block_number->second))
            {
                LOG_DEBUG(log, "Removing from filesystem the outdated WAL file {}", it->name());
                disk_ptr->removeFile(relative_data_path + it->name());
                ++cleared_count;
            }
        }
    }

    return cleared_count;
}

size_t MergeTreeData::clearEmptyParts()
{
    if (!getSettings()->remove_empty_parts)
        return 0;

    size_t cleared_count = 0;
    auto parts = getDataPartsVectorForInternalUsage();
    for (const auto & part : parts)
    {
        if (part->rows_count != 0)
            continue;

        /// Do not try to drop uncommitted parts. If the newest tx doesn't see it that is probably hasn't been committed jet
        if (!part->version.getCreationTID().isPrehistoric() && !part->version.isVisible(TransactionLog::instance().getLatestSnapshot()))
            continue;

        /// Don't drop empty parts that cover other parts
        /// Otherwise covered parts resurrect
        {
            auto lock = lockParts();
            if (part->getState() != DataPartState::Active)
                continue;

            DataPartsVector covered_parts = getCoveredOutdatedParts(part, lock);
            if (!covered_parts.empty())
                continue;
        }

        LOG_INFO(log, "Will drop empty part {}", part->name);

        dropPartNoWaitNoThrow(part->name);
        ++cleared_count;
    }
    return cleared_count;
}

void MergeTreeData::rename(const String & new_table_path, const StorageID & new_table_id)
{
    auto disks = getStoragePolicy()->getDisks();

    for (const auto & disk : disks)
    {
        if (disk->exists(new_table_path))
            throw Exception(ErrorCodes::DIRECTORY_ALREADY_EXISTS, "Target path already exists: {}", fullPath(disk, new_table_path));
    }

    {
        /// Relies on storage path, so we drop it during rename
        /// it will be recreated automatiaclly.
        std::lock_guard wal_lock(write_ahead_log_mutex);
        if (write_ahead_log)
        {
            write_ahead_log->shutdown();
            write_ahead_log.reset();
        }
    }

    for (const auto & disk : disks)
    {
        auto new_table_path_parent = parentPath(new_table_path);
        disk->createDirectories(new_table_path_parent);
        disk->moveDirectory(relative_data_path, new_table_path);
    }

    if (!getStorageID().hasUUID())
        getContext()->dropCaches();

    /// TODO: remove const_cast
    for (const auto & part : data_parts_by_info)
    {
        auto & part_mutable = const_cast<IMergeTreeDataPart &>(*part);
        part_mutable.getDataPartStorage().changeRootPath(relative_data_path, new_table_path);
    }

    relative_data_path = new_table_path;

    renameInMemory(new_table_id);
}

void MergeTreeData::renameInMemory(const StorageID & new_table_id)
{
    IStorage::renameInMemory(new_table_id);
    log.store(new_table_id.getNameForLogs());
}

void MergeTreeData::dropAllData()
{
    /// In case there is read-only/write-once disk we cannot allow to call dropAllData(), but dropping tables is allowed.
    ///
    /// Note, that one may think that drop on write-once disk should be
    /// supported, since it is pretty trivial to implement
    /// MetadataStorageFromPlainObjectStorageTransaction::removeDirectory(),
    /// however removing part requires moveDirectory() as well.
    if (isStaticStorage())
        return;

    LOG_TRACE(log, "dropAllData: waiting for locks.");
    auto settings_ptr = getSettings();

    auto lock = lockParts();

    DataPartsVector all_parts;
    for (auto it = data_parts_by_info.begin(); it != data_parts_by_info.end(); ++it)
    {
        modifyPartState(it, DataPartState::Deleting);
        all_parts.push_back(*it);
    }

    {
        std::lock_guard wal_lock(write_ahead_log_mutex);
        if (write_ahead_log)
            write_ahead_log->shutdown();
    }
    /// Tables in atomic databases have UUID and stored in persistent locations.
    /// No need to drop caches (that are keyed by filesystem path) because collision is not possible.
    if (!getStorageID().hasUUID())
        getContext()->dropCaches();

    /// Removing of each data part before recursive removal of directory is to speed-up removal, because there will be less number of syscalls.
    NameSet part_names_failed;
    try
    {
        LOG_TRACE(log, "dropAllData: removing data parts (count {}) from filesystem.", all_parts.size());
        clearPartsFromFilesystem(all_parts, true, &part_names_failed);

        LOG_TRACE(log, "dropAllData: removing all data parts from memory.");
        data_parts_indexes.clear();
        all_data_dropped = true;
    }
    catch (...)
    {
        /// Removing from memory only successfully removed parts from disk
        /// Parts removal process can be important and on the next try it's better to try to remove
        /// them instead of remove recursive call.
        LOG_WARNING(log, "dropAllData: got exception removing parts from disk, removing successfully removed parts from memory.");
        for (const auto & part : all_parts)
        {
            if (!part_names_failed.contains(part->name))
                data_parts_indexes.erase(part->info);
        }

        throw;
    }

    LOG_INFO(log, "dropAllData: clearing temporary directories");
    clearOldTemporaryDirectories(0, {"tmp_", "delete_tmp_", "tmp-fetch_"});

    column_sizes.clear();

    for (const auto & disk : getDisks())
    {
        if (disk->isBroken())
            continue;

        LOG_INFO(log, "dropAllData: remove format_version.txt, detached, moving and write ahead logs");
        disk->removeFileIfExists(fs::path(relative_data_path) / FORMAT_VERSION_FILE_NAME);

        if (disk->exists(fs::path(relative_data_path) / DETACHED_DIR_NAME))
            disk->removeRecursive(fs::path(relative_data_path) / DETACHED_DIR_NAME);

        if (disk->exists(fs::path(relative_data_path) / MOVING_DIR_NAME))
            disk->removeRecursive(fs::path(relative_data_path) / MOVING_DIR_NAME);

        try
        {
            MergeTreeWriteAheadLog::dropAllWriteAheadLogs(disk, relative_data_path);

            if (!disk->isDirectoryEmpty(relative_data_path) &&
                supportsReplication() && disk->supportZeroCopyReplication()
                && settings_ptr->allow_remote_fs_zero_copy_replication)
            {
                std::vector<std::string> files_left;
                disk->listFiles(relative_data_path, files_left);

                throw Exception(
                                ErrorCodes::ZERO_COPY_REPLICATION_ERROR,
                                "Directory {} with table {} not empty (files [{}]) after drop. Will not drop.",
                                relative_data_path, getStorageID().getNameForLogs(), fmt::join(files_left, ", "));
            }

            LOG_INFO(log, "dropAllData: removing table directory recursive to cleanup garbage");
            disk->removeRecursive(relative_data_path);
        }
        catch (const fs::filesystem_error & e)
        {
            if (e.code() == std::errc::no_such_file_or_directory)
            {
                /// If the file is already deleted, log the error message and do nothing.
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
            else
                throw;
        }
    }

    setDataVolume(0, 0, 0);

    LOG_TRACE(log, "dropAllData: done.");
}

void MergeTreeData::dropIfEmpty()
{
    LOG_TRACE(log, "dropIfEmpty");

    auto lock = lockParts();

    if (!data_parts_by_info.empty())
        return;

    try
    {
        for (const auto & disk : getDisks())
        {
            if (disk->isBroken())
                continue;
            /// Non recursive, exception is thrown if there are more files.
            disk->removeFileIfExists(fs::path(relative_data_path) / MergeTreeData::FORMAT_VERSION_FILE_NAME);
            disk->removeDirectory(fs::path(relative_data_path) / MergeTreeData::DETACHED_DIR_NAME);
            disk->removeDirectory(relative_data_path);
        }
    }
    catch (...)
    {
        // On unsuccessful creation of ReplicatedMergeTree table with multidisk configuration some files may not exist.
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

namespace
{

/// Conversion that is allowed for serializable key (primary key, sorting key).
/// Key should be serialized in the same way after conversion.
/// NOTE: The list is not complete.
bool isSafeForKeyConversion(const IDataType * from, const IDataType * to)
{
    if (from->getName() == to->getName())
        return true;

    /// Enums are serialized in partition key as numbers - so conversion from Enum to number is Ok.
    /// But only for types of identical width because they are serialized as binary in minmax index.
    /// But not from number to Enum because Enum does not necessarily represents all numbers.

    if (const auto * from_enum8 = typeid_cast<const DataTypeEnum8 *>(from))
    {
        if (const auto * to_enum8 = typeid_cast<const DataTypeEnum8 *>(to))
            return to_enum8->contains(*from_enum8);
        if (typeid_cast<const DataTypeInt8 *>(to))
            return true;    // NOLINT
        return false;
    }

    if (const auto * from_enum16 = typeid_cast<const DataTypeEnum16 *>(from))
    {
        if (const auto * to_enum16 = typeid_cast<const DataTypeEnum16 *>(to))
            return to_enum16->contains(*from_enum16);
        if (typeid_cast<const DataTypeInt16 *>(to))
            return true;    // NOLINT
        return false;
    }

    if (const auto * from_lc = typeid_cast<const DataTypeLowCardinality *>(from))
        return from_lc->getDictionaryType()->equals(*to);

    if (const auto * to_lc = typeid_cast<const DataTypeLowCardinality *>(to))
        return to_lc->getDictionaryType()->equals(*from);

    return false;
}

/// Special check for alters of VersionedCollapsingMergeTree version column
void checkVersionColumnTypesConversion(const IDataType * old_type, const IDataType * new_type, const String column_name)
{
    /// Check new type can be used as version
    if (!new_type->canBeUsedAsVersion())
        throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                        "Cannot alter version column {} to type {} because version column must be "
                        "of an integer type or of type Date or DateTime" , backQuoteIfNeed(column_name),
                        new_type->getName());

    auto which_new_type = WhichDataType(new_type);
    auto which_old_type = WhichDataType(old_type);

    /// Check alter to different sign or float -> int and so on
    if ((which_old_type.isInt() && !which_new_type.isInt())
        || (which_old_type.isUInt() && !which_new_type.isUInt())
        || (which_old_type.isDate() && !which_new_type.isDate())
        || (which_old_type.isDate32() && !which_new_type.isDate32())
        || (which_old_type.isDateTime() && !which_new_type.isDateTime())
        || (which_old_type.isFloat() && !which_new_type.isFloat()))
    {
        throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN, "Cannot alter version column {} from type {} to type {} "
                        "because new type will change sort order of version column. "
                        "The only possible conversion is expansion of the number of bytes of the current type.",
                        backQuoteIfNeed(column_name), old_type->getName(), new_type->getName());
    }

    /// Check alter to smaller size: UInt64 -> UInt32 and so on
    if (new_type->getSizeOfValueInMemory() < old_type->getSizeOfValueInMemory())
    {
        throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN, "Cannot alter version column {} from type {} to type {} "
                        "because new type is smaller than current in the number of bytes. "
                        "The only possible conversion is expansion of the number of bytes of the current type.",
                        backQuoteIfNeed(column_name), old_type->getName(), new_type->getName());
    }
}

}

void MergeTreeData::checkAlterIsPossible(const AlterCommands & commands, ContextPtr local_context) const
{
    /// Check that needed transformations can be applied to the list of columns without considering type conversions.
    StorageInMemoryMetadata new_metadata = getInMemoryMetadata();
    StorageInMemoryMetadata old_metadata = getInMemoryMetadata();

    const auto & settings = local_context->getSettingsRef();
    const auto & settings_from_storage = getSettings();

    if (!settings.allow_non_metadata_alters)
    {
        auto mutation_commands = commands.getMutationCommands(new_metadata, settings.materialize_ttl_after_modify, getContext());

        if (!mutation_commands.empty())
            throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                            "The following alter commands: '{}' will modify data on disk, "
                            "but setting `allow_non_metadata_alters` is disabled",
                            queryToString(mutation_commands.ast()));
    }
    commands.apply(new_metadata, getContext());

    for (const auto & disk : getDisks())
        if (!disk->supportsHardLinks())
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "ALTER TABLE is not supported for immutable disk '{}'", disk->getName());

    /// Set of columns that shouldn't be altered.
    NameSet columns_alter_type_forbidden;

    /// Primary key columns can be ALTERed only if they are used in the key as-is
    /// (and not as a part of some expression) and if the ALTER only affects column metadata.
    NameSet columns_alter_type_metadata_only;

    /// Columns to check that the type change is safe for partition key.
    NameSet columns_alter_type_check_safe_for_partition;

    if (old_metadata.hasPartitionKey())
    {
        /// Forbid altering columns inside partition key expressions because it can change partition ID format.
        auto partition_key_expr = old_metadata.getPartitionKey().expression;
        for (const auto & action : partition_key_expr->getActions())
        {
            for (const auto * child : action.node->children)
                columns_alter_type_forbidden.insert(child->result_name);
        }

        /// But allow to alter columns without expressions under certain condition.
        for (const String & col : partition_key_expr->getRequiredColumns())
            columns_alter_type_check_safe_for_partition.insert(col);
    }

    if (old_metadata.hasSortingKey())
    {
        auto sorting_key_expr = old_metadata.getSortingKey().expression;
        for (const auto & action : sorting_key_expr->getActions())
        {
            for (const auto * child : action.node->children)
                columns_alter_type_forbidden.insert(child->result_name);
        }
        for (const String & col : sorting_key_expr->getRequiredColumns())
            columns_alter_type_metadata_only.insert(col);

        /// We don't process sample_by_ast separately because it must be among the primary key columns
        /// and we don't process primary_key_expr separately because it is a prefix of sorting_key_expr.
    }
    if (!merging_params.sign_column.empty())
        columns_alter_type_forbidden.insert(merging_params.sign_column);

    /// All of the above.
    NameSet columns_in_keys;
    columns_in_keys.insert(columns_alter_type_forbidden.begin(), columns_alter_type_forbidden.end());
    columns_in_keys.insert(columns_alter_type_metadata_only.begin(), columns_alter_type_metadata_only.end());
    columns_in_keys.insert(columns_alter_type_check_safe_for_partition.begin(), columns_alter_type_check_safe_for_partition.end());

    std::unordered_map<String, String> columns_in_indices;
    for (const auto & index : old_metadata.getSecondaryIndices())
    {
        for (const String & col : index.expression->getRequiredColumns())
            columns_in_indices.emplace(col, index.name);
    }

    std::unordered_map<String, String> columns_in_projections;
    for (const auto & projection : old_metadata.getProjections())
    {
        for (const String & col : projection.getRequiredColumns())
            columns_in_projections.emplace(col, projection.name);
    }

    NameSet dropped_columns;

    std::map<String, const IDataType *> old_types;
    for (const auto & column : old_metadata.getColumns().getAllPhysical())
        old_types.emplace(column.name, column.type.get());

    NamesAndTypesList columns_to_check_conversion;
    auto name_deps = getDependentViewsByColumn(local_context);
    for (const AlterCommand & command : commands)
    {
        /// Just validate partition expression
        if (command.partition)
        {
            getPartitionIDFromQuery(command.partition, getContext());
        }

        if (command.column_name == merging_params.version_column)
        {
            /// Some type changes for version column is allowed despite it's a part of sorting key
            if (command.type == AlterCommand::MODIFY_COLUMN)
            {
                const IDataType * new_type = command.data_type.get();
                const IDataType * old_type = old_types[command.column_name];

                if (new_type)
                    checkVersionColumnTypesConversion(old_type, new_type, command.column_name);

                /// No other checks required
                continue;
            }
            else if (command.type == AlterCommand::DROP_COLUMN)
            {
                throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                    "Trying to ALTER DROP version {} column", backQuoteIfNeed(command.column_name));
            }
            else if (command.type == AlterCommand::RENAME_COLUMN)
            {
                throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                    "Trying to ALTER RENAME version {} column", backQuoteIfNeed(command.column_name));
            }
        }

        if (command.type == AlterCommand::MODIFY_ORDER_BY && !is_custom_partitioned)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "ALTER MODIFY ORDER BY is not supported for default-partitioned tables created with the old syntax");
        }
        if (command.type == AlterCommand::MODIFY_TTL && !is_custom_partitioned)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "ALTER MODIFY TTL is not supported for default-partitioned tables created with the old syntax");
        }
        if (command.type == AlterCommand::MODIFY_SAMPLE_BY)
        {
            if (!is_custom_partitioned)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "ALTER MODIFY SAMPLE BY is not supported for default-partitioned tables created with the old syntax");

            checkSampleExpression(new_metadata, getSettings()->compatibility_allow_sampling_expression_not_in_primary_key,
                                  getSettings()->check_sample_column_is_correct);
        }
        if (command.type == AlterCommand::ADD_INDEX && !is_custom_partitioned)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "ALTER ADD INDEX is not supported for tables with the old syntax");
        }
        if (command.type == AlterCommand::ADD_PROJECTION)
        {
            if (!is_custom_partitioned)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "ALTER ADD PROJECTION is not supported for streams with the old syntax");

            /// TODO: implement it the main issue in DataPartsExchange (not able to send directories metadata)
            if (supportsReplication() && getSettings()->allow_remote_fs_zero_copy_replication)
            {
                auto storage_policy = getStoragePolicy();
                auto disks = storage_policy->getDisks();
                for (const auto & disk : disks)
                {
                    if (disk->supportZeroCopyReplication())
                        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ALTER ADD PROJECTION is not supported when zero-copy replication is enabled for table. "
                                        "Currently disk '{}' supports zero copy replication", disk->getName());
                }
            }
        }
        if (command.type == AlterCommand::RENAME_COLUMN)
        {
            if (columns_in_keys.contains(command.column_name))
            {
                throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                                "Trying to ALTER RENAME key {} column which is a part of key expression",
                                backQuoteIfNeed(command.column_name));
            }

            /// Don't check columns in indices here. RENAME works fine with index columns.

            if (auto it = columns_in_projections.find(command.column_name); it != columns_in_projections.end())
            {
                throw Exception(
                    ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                    "Trying to ALTER RENAME {} column which is a part of projection {}",
                    backQuoteIfNeed(command.column_name),
                    it->second);
            }
        }
        else if (command.type == AlterCommand::DROP_COLUMN)
        {
            if (columns_in_keys.contains(command.column_name))
            {
                throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                    "Trying to ALTER DROP key {} column which is a part of key expression", backQuoteIfNeed(command.column_name));
            }

            /// Don't check columns in indices or projections here. If required columns of indices
            /// or projections get dropped, it will be checked later in AlterCommands::apply. This
            /// allows projections with * to drop columns. One example can be found in
            /// 02691_drop_column_with_projections_replicated.sql.

            if (!command.clear)
            {
                const auto & deps_mv = name_deps[command.column_name];
                if (!deps_mv.empty())
                {
                    throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                        "Trying to ALTER DROP column {} which is referenced by materialized view {}",
                        backQuoteIfNeed(command.column_name), toString(deps_mv));
                }
            }
            if (old_metadata.columns.has(command.column_name))
            {
                dropped_columns.emplace(command.column_name);
            }
            else
            {
                const auto & nested = old_metadata.columns.getNested(command.column_name);
                for (const auto & nested_column : nested)
                    dropped_columns.emplace(nested_column.name);
            }
        }
        else if (command.type == AlterCommand::RESET_SETTING)
        {
            for (const auto & reset_setting : command.settings_resets)
            {
                if (!settings_from_storage->has(reset_setting))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                    "Cannot reset setting '{}' because it doesn't exist for MergeTree engines family",
                                    reset_setting);
            }
        }
        else if (command.isRequireMutationStage(getInMemoryMetadata()))
        {
            /// This alter will override data on disk. Let's check that it doesn't
            /// modify immutable column.
            if (columns_alter_type_forbidden.contains(command.column_name))
                throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN, "ALTER of key column {} is forbidden",
                    backQuoteIfNeed(command.column_name));

            if (auto it = columns_in_indices.find(command.column_name); it != columns_in_indices.end())
            {
                throw Exception(
                    ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                    "Trying to ALTER {} column which is a part of index {}",
                    backQuoteIfNeed(command.column_name),
                    it->second);
            }

            /// Don't check columns in projections here. If required columns of projections get
            /// modified, it will be checked later in AlterCommands::apply.

            if (command.type == AlterCommand::MODIFY_COLUMN)
            {
                if (columns_alter_type_check_safe_for_partition.contains(command.column_name))
                {
                    auto it = old_types.find(command.column_name);

                    assert(it != old_types.end());
                    if (!isSafeForKeyConversion(it->second, command.data_type.get()))
                        throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                                        "ALTER of partition key column {} from type {} "
                                        "to type {} is not safe because it can change the representation "
                                        "of partition key", backQuoteIfNeed(command.column_name),
                                        it->second->getName(), command.data_type->getName());
                }

                if (columns_alter_type_metadata_only.contains(command.column_name))
                {
                    auto it = old_types.find(command.column_name);
                    assert(it != old_types.end());
                    if (!isSafeForKeyConversion(it->second, command.data_type.get()))
                        throw Exception(ErrorCodes::ALTER_OF_COLUMN_IS_FORBIDDEN,
                                        "ALTER of key column {} from type {} "
                                        "to type {} is not safe because it can change the representation "
                                        "of primary key", backQuoteIfNeed(command.column_name),
                                        it->second->getName(), command.data_type->getName());
                }

                if (old_metadata.getColumns().has(command.column_name))
                {
                    columns_to_check_conversion.push_back(
                        new_metadata.getColumns().getPhysical(command.column_name));
                }
            }
        }
    }

    checkColumnFilenamesForCollision(new_metadata, /*throw_on_error=*/ true);
    checkProperties(new_metadata, old_metadata);
    checkTTLExpressions(new_metadata, old_metadata);

    if (!columns_to_check_conversion.empty())
    {
        auto old_header = old_metadata.getSampleBlock();
        performRequiredConversions(old_header, columns_to_check_conversion, getContext());
    }

    if (old_metadata.hasSettingsChanges())
    {
        const auto current_changes = old_metadata.getSettingsChanges()->as<const ASTSetQuery &>().changes;
        const auto & new_changes = new_metadata.settings_changes->as<const ASTSetQuery &>().changes;
        local_context->checkMergeTreeSettingsConstraints(*settings_from_storage, new_changes);

        for (const auto & changed_setting : new_changes)
        {
            const auto & setting_name = changed_setting.name;
            const auto & new_value = changed_setting.value;
            MergeTreeSettings::checkCanSet(setting_name, new_value);
            const Field * current_value = current_changes.tryGet(setting_name);

            if ((!current_value || *current_value != new_value)
                && MergeTreeSettings::isReadonlySetting(setting_name))
            {
                throw Exception(ErrorCodes::READONLY_SETTING, "Setting '{}' is readonly for storage '{}'", setting_name, getName());
            }

            if (!current_value && MergeTreeSettings::isPartFormatSetting(setting_name))
            {
                MergeTreeSettings copy = *getSettings();
                copy.applyChange(changed_setting);
                String reason;
                if (!canUsePolymorphicParts(copy, &reason) && !reason.empty())
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Can't change settings. Reason: {}", reason);
            }

            if (setting_name == "storage_policy")
                checkStoragePolicy(getContext()->getStoragePolicy(new_value.safeGet<String>()));
        }

        /// Check if it is safe to reset the settings
        for (const auto & current_setting : current_changes)
        {
            const auto & setting_name = current_setting.name;
            const Field * new_value = new_changes.tryGet(setting_name);
            /// Prevent unsetting readonly setting
            if (MergeTreeSettings::isReadonlySetting(setting_name) && !new_value)
            {
                throw Exception(ErrorCodes::READONLY_SETTING, "Setting '{}' is readonly for storage '{}'", setting_name, getName());
            }

            if (MergeTreeSettings::isPartFormatSetting(setting_name) && !new_value)
            {
                /// Use default settings + new and check if doesn't affect part format settings
                auto copy = getDefaultSettings();
                copy->applyChanges(new_changes);
                String reason;
                if (!canUsePolymorphicParts(*copy, &reason) && !reason.empty())
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Can't change settings. Reason: {}", reason);
            }

        }
    }

    for (const auto & part : getDataPartsVectorForInternalUsage())
    {
        bool at_least_one_column_rest = false;
        for (const auto & column : part->getColumns())
        {
            if (!dropped_columns.contains(column.name))
            {
                at_least_one_column_rest = true;
                break;
            }
        }
        if (!at_least_one_column_rest)
        {
            std::string postfix;
            if (dropped_columns.size() > 1)
                postfix = "s";
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Cannot drop or clear column{} '{}', because all columns "
                            "in part '{}' will be removed from disk. Empty parts are not allowed",
                            postfix, boost::algorithm::join(dropped_columns, ", "), part->name);
        }
    }
}


void MergeTreeData::checkMutationIsPossible(const MutationCommands & /*commands*/, const Settings & /*settings*/) const
{
    for (const auto & disk : getDisks())
        if (!disk->supportsHardLinks())
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Mutations are not supported for immutable disk '{}'", disk->getName());
}

MergeTreeDataPartFormat MergeTreeData::choosePartFormat(size_t bytes_uncompressed, size_t rows_count, bool only_on_disk) const
{
    using PartType = MergeTreeDataPartType;
    using PartStorageType = MergeTreeDataPartStorageType;

     const auto settings = getSettings();
     if (!canUsePolymorphicParts(*settings))
        return {PartType::Wide, PartStorageType::Full};

    auto satisfies = [&](const auto & min_bytes_for, const auto & min_rows_for)
    {
        return bytes_uncompressed < min_bytes_for || rows_count < min_rows_for;
    };

    if (!only_on_disk && satisfies(settings->min_bytes_for_compact_part, settings->min_rows_for_compact_part))
        return {PartType::InMemory, PartStorageType::Full};

    auto part_type = PartType::Wide;
    if (satisfies(settings->min_bytes_for_wide_part, settings->min_rows_for_wide_part))
        part_type = PartType::Compact;

    return {part_type, PartStorageType::Full};
}

MergeTreeDataPartFormat MergeTreeData::choosePartFormatOnDisk(size_t bytes_uncompressed, size_t rows_count) const
{
    return choosePartFormat(bytes_uncompressed, rows_count, true);
}

MergeTreeDataPartBuilder MergeTreeData::getDataPartBuilder(
    const String & name, const VolumePtr & volume, const String & part_dir) const
{
    return MergeTreeDataPartBuilder(*this, name, volume, relative_data_path, part_dir);
}

void MergeTreeData::changeSettings(
        const ASTPtr & new_settings,
        AlterLockHolder & /* table_lock_holder */)
{
    if (new_settings)
    {
        bool has_storage_policy_changed = false;

        const auto & new_changes = new_settings->as<const ASTSetQuery &>().changes;

        for (const auto & change : new_changes)
        {
            if (change.name == "storage_policy")
            {
                StoragePolicyPtr new_storage_policy = getContext()->getStoragePolicy(change.value.safeGet<String>());
                StoragePolicyPtr old_storage_policy = getStoragePolicy();

                /// StoragePolicy of different version or name is guaranteed to have different pointer
                if (new_storage_policy != old_storage_policy)
                {
                    checkStoragePolicy(new_storage_policy);

                    std::unordered_set<String> all_diff_disk_names;
                    for (const auto & disk : new_storage_policy->getDisks())
                        all_diff_disk_names.insert(disk->getName());
                    for (const auto & disk : old_storage_policy->getDisks())
                        all_diff_disk_names.erase(disk->getName());

                    for (const String & disk_name : all_diff_disk_names)
                    {
                        auto disk = new_storage_policy->getDiskByName(disk_name);
                        if (disk->exists(relative_data_path))
                            throw Exception(ErrorCodes::LOGICAL_ERROR, "New storage policy contain disks which already contain data of a stream with the same name");
                    }

                    for (const String & disk_name : all_diff_disk_names)
                    {
                        auto disk = new_storage_policy->getDiskByName(disk_name);
                        disk->createDirectories(relative_data_path);
                        disk->createDirectories(fs::path(relative_data_path) / MergeTreeData::DETACHED_DIR_NAME);
                    }
                    /// FIXME how would that be done while reloading configuration???

                    has_storage_policy_changed = true;
                }
            }
        }

        /// Reset to default settings before applying existing.
        auto copy = getDefaultSettings();
        copy->applyChanges(new_changes);
        copy->sanityCheck(getContext()->getSettingsRef());

        storage_settings.set(std::move(copy));
        StorageInMemoryMetadata new_metadata = getInMemoryMetadata();
        new_metadata.setSettingsChanges(new_settings);
        setInMemoryMetadata(new_metadata);

        if (has_storage_policy_changed)
            startBackgroundMovesIfNeeded();
    }
}

void MergeTreeData::PartsTemporaryRename::addPart(const String & old_name, const String & new_name, const DiskPtr & disk)
{
    old_and_new_names.push_back({old_name, new_name, disk});
}

void MergeTreeData::PartsTemporaryRename::tryRenameAll()
{
    renamed = true;
    for (size_t i = 0; i < old_and_new_names.size(); ++i)
    {
        try
        {
            const auto & [old_name, new_name, disk] = old_and_new_names[i];
            if (old_name.empty() || new_name.empty())
                throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Empty part name. Most likely it's a bug.");
            const auto full_path = fs::path(storage.relative_data_path) / source_dir;
            disk->moveFile(fs::path(full_path) / old_name, fs::path(full_path) / new_name);
        }
        catch (...)
        {
            old_and_new_names.resize(i);
            LOG_WARNING(storage.log, "Cannot rename parts to perform operation on them: {}", getCurrentExceptionMessage(false));
            throw;
        }
    }
}

MergeTreeData::PartsTemporaryRename::~PartsTemporaryRename()
{
    // TODO what if server had crashed before this destructor was called?
    if (!renamed)
        return;
    for (const auto & [old_name, new_name, disk] : old_and_new_names)
    {
        if (old_name.empty())
            continue;

        try
        {
            const String full_path = fs::path(storage.relative_data_path) / source_dir;
            disk->moveFile(fs::path(full_path) / new_name, fs::path(full_path) / old_name);
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

MergeTreeData::PartHierarchy MergeTreeData::getPartHierarchy(
    const MergeTreePartInfo & part_info,
    DataPartState state,
    DataPartsLock & /* data_parts_lock */) const
{
    PartHierarchy result;

    /// Parts contained in the part are consecutive in data_parts, intersecting the insertion place for the part itself.
    auto it_middle = data_parts_by_state_and_info.lower_bound(DataPartStateAndInfo{state, part_info});
    auto committed_parts_range = getDataPartsStateRange(state);

    /// Go to the left.
    DataPartIteratorByStateAndInfo begin = it_middle;
    while (begin != committed_parts_range.begin())
    {
        auto prev = std::prev(begin);

        if (!part_info.contains((*prev)->info))
        {
            if ((*prev)->info.contains(part_info))
            {
                result.covering_parts.push_back(*prev);
            }
            else if (!part_info.isDisjoint((*prev)->info))
            {
                result.intersected_parts.push_back(*prev);
            }

            break;
        }

        begin = prev;
    }

    std::reverse(result.covering_parts.begin(), result.covering_parts.end());

    /// Go to the right.
    DataPartIteratorByStateAndInfo end = it_middle;
    while (end != committed_parts_range.end())
    {
        if ((*end)->info == part_info)
        {
            result.duplicate_part = *end;
            result.covering_parts.clear();
            return result;
        }

        if (!part_info.contains((*end)->info))
        {
            if ((*end)->info.contains(part_info))
            {
                result.covering_parts.push_back(*end);
            }
            else if (!part_info.isDisjoint((*end)->info))
            {
                result.intersected_parts.push_back(*end);
            }

            break;
        }

        ++end;
    }

    result.covered_parts.insert(result.covered_parts.end(), begin, end);

    return result;
}

MergeTreeData::DataPartsVector MergeTreeData::getCoveredOutdatedParts(
    const DataPartPtr & part,
    DataPartsLock & data_parts_lock) const
{
    part->assertState({DataPartState::Active, DataPartState::PreActive});
    PartHierarchy hierarchy = getPartHierarchy(part->info, DataPartState::Outdated, data_parts_lock);

    if (hierarchy.duplicate_part)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected duplicate part {}. It is a bug.", hierarchy.duplicate_part->getNameWithState());

    return hierarchy.covered_parts;
}

MergeTreeData::DataPartsVector MergeTreeData::getActivePartsToReplace(
    const MergeTreePartInfo & new_part_info,
    const String & new_part_name,
    DataPartPtr & out_covering_part,
    DataPartsLock & data_parts_lock) const
{
    PartHierarchy hierarchy = getPartHierarchy(new_part_info, DataPartState::Active, data_parts_lock);

    if (!hierarchy.intersected_parts.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Part {} intersects part {}. It is a bug.",
                        new_part_name, hierarchy.intersected_parts.back()->getNameWithState());

    if (hierarchy.duplicate_part)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected duplicate part {}. It is a bug.", hierarchy.duplicate_part->getNameWithState());

    if (!hierarchy.covering_parts.empty())
        out_covering_part = std::move(hierarchy.covering_parts.back());

    return std::move(hierarchy.covered_parts);
}

void MergeTreeData::checkPartPartition(MutableDataPartPtr & part, DataPartsLock & lock) const
{
    if (DataPartPtr existing_part_in_partition = getAnyPartInPartition(part->info.partition_id, lock))
    {
        if (part->partition.value != existing_part_in_partition->partition.value)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Partition value mismatch between two parts with the same partition ID. "
                "Existing part: {}, newly added part: {}", existing_part_in_partition->name, part->name);
    }
}

void MergeTreeData::checkPartDuplicate(MutableDataPartPtr & part, Transaction & transaction, DataPartsLock & /*lock*/) const
{
    auto it_duplicate = data_parts_by_info.find(part->info);

    if (it_duplicate != data_parts_by_info.end())
    {
        if ((*it_duplicate)->checkState({DataPartState::Outdated, DataPartState::Deleting}))
            throw Exception(ErrorCodes::PART_IS_TEMPORARILY_LOCKED, "Part {} already exists, but it will be deleted soon",
                            (*it_duplicate)->getNameWithState());

        if (transaction.txn)
            throw Exception(ErrorCodes::SERIALIZATION_ERROR, "Part {} already exists", (*it_duplicate)->getNameWithState());

        throw Exception(ErrorCodes::DUPLICATE_DATA_PART, "Part {} already exists", (*it_duplicate)->getNameWithState());
    }
}

void MergeTreeData::preparePartForCommit(MutableDataPartPtr & part, Transaction & out_transaction, bool need_rename)
{
    part->is_temp = false;
    part->setState(DataPartState::PreActive);

    assert([&]()
           {
               String dir_name = fs::path(part->getDataPartStorage().getRelativePath()).filename();
               bool may_be_cleaned_up = dir_name.starts_with("tmp_") || dir_name.starts_with("tmp-fetch_");
               return !may_be_cleaned_up || temporary_parts.contains(dir_name);
           }());

    if (need_rename)
        part->renameTo(part->name, true);

    data_parts_indexes.insert(part);
    out_transaction.addPart(part);
}

bool MergeTreeData::addTempPart(
    MutableDataPartPtr & part,
    Transaction & out_transaction,
    DataPartsLock & lock,
    DataPartsVector * out_covered_parts)
{
    LOG_TRACE(log, "Adding temporary part from directory {} with name {}.", part->getDataPartStorage().getPartDirectory(), part->name);
    if (&out_transaction.data != this)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeTreeData::Transaction for one table cannot be used with another. It is a bug.");

    if (part->hasLightweightDelete())
        has_lightweight_delete_parts.store(true);

    checkPartPartition(part, lock);
    checkPartDuplicate(part, out_transaction, lock);

    DataPartPtr covering_part;
    DataPartsVector covered_parts = getActivePartsToReplace(part->info, part->name, covering_part, lock);

    if (covering_part)
    {
        LOG_WARNING(log, "Tried to add obsolete part {} covered by {}", part->name, covering_part->getNameWithState());
        return false;
    }

    /// All checks are passed. Now we can rename the part on disk.
    /// So, we maintain invariant: if a non-temporary part in filesystem then it is in data_parts
    preparePartForCommit(part, out_transaction, /* need_rename = */false);

    if (out_covered_parts)
    {
        out_covered_parts->reserve(covered_parts.size());

        for (DataPartPtr & covered_part : covered_parts)
            out_covered_parts->emplace_back(std::move(covered_part));
    }

    return true;
}


bool MergeTreeData::renameTempPartAndReplaceImpl(
    MutableDataPartPtr & part,
    Transaction & out_transaction,
    DataPartsLock & lock,
    DataPartsVector * out_covered_parts)
{
    LOG_TRACE(log, "Renaming temporary part {} to {} with tid {}.", part->getDataPartStorage().getPartDirectory(), part->name, out_transaction.getTID());

    if (&out_transaction.data != this)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeTreeData::Transaction for one table cannot be used with another. It is a bug.");

    part->assertState({DataPartState::Temporary});
    checkPartPartition(part, lock);
    checkPartDuplicate(part, out_transaction, lock);

    PartHierarchy hierarchy = getPartHierarchy(part->info, DataPartState::Active, lock);

    if (!hierarchy.intersected_parts.empty())
    {
        // Drop part|partition operation inside some transactions sees some stale snapshot from the time when transactions has been started.
        // So such operation may attempt to delete already outdated part. In this case, this outdated part is most likely covered by the other part and intersection may occur.
        // Part mayght be outdated due to merge|mutation|update|optimization operations.
        if (part->isEmpty() || (hierarchy.intersected_parts.size() == 1 && hierarchy.intersected_parts.back()->isEmpty()))
        {
            throw Exception(ErrorCodes::SERIALIZATION_ERROR, "Part {} intersects part {}. One of them is empty part. "
                            "That is a race between drop operation under transaction and a merge/mutation.",
                            part->name, hierarchy.intersected_parts.back()->getNameWithState());
        }

        throw Exception(ErrorCodes::LOGICAL_ERROR, "Part {} intersects part {}. There are {} intersected parts. It is a bug.",
                        part->name, hierarchy.intersected_parts.back()->getNameWithState(), hierarchy.intersected_parts.size());
    }

    if (part->hasLightweightDelete())
        has_lightweight_delete_parts.store(true);

    /// All checks are passed. Now we can rename the part on disk.
    /// So, we maintain invariant: if a non-temporary part in filesystem then it is in data_parts
    preparePartForCommit(part, out_transaction, /* need_rename */ true);

    if (out_covered_parts)
    {
        out_covered_parts->reserve(out_covered_parts->size() + hierarchy.covered_parts.size());
        std::move(hierarchy.covered_parts.begin(), hierarchy.covered_parts.end(), std::back_inserter(*out_covered_parts));
    }

    return true;
}

bool MergeTreeData::renameTempPartAndReplaceUnlocked(
    MutableDataPartPtr & part,
    Transaction & out_transaction,
    DataPartsLock & lock,
    DataPartsVector * out_covered_parts)
{
    return renameTempPartAndReplaceImpl(part, out_transaction, lock, out_covered_parts);
}

MergeTreeData::DataPartsVector MergeTreeData::renameTempPartAndReplace(
    MutableDataPartPtr & part,
    Transaction & out_transaction)
{
    auto part_lock = lockParts();
    DataPartsVector covered_parts;
    renameTempPartAndReplaceImpl(part, out_transaction, part_lock, &covered_parts);
    return covered_parts;
}

bool MergeTreeData::renameTempPartAndAdd(
    MutableDataPartPtr & part,
    Transaction & out_transaction,
    DataPartsLock & lock)
{
    DataPartsVector covered_parts;

    if (!renameTempPartAndReplaceImpl(part, out_transaction, lock, &covered_parts))
        return false;

    if (!covered_parts.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Added part {} covers {} existing part(s) (including {})",
            part->name, covered_parts.size(), covered_parts[0]->name);

    return true;
}

void MergeTreeData::removePartsFromWorkingSet(MergeTreeTransaction * txn, const MergeTreeData::DataPartsVector & remove, bool clear_without_timeout, [[maybe_unused]] DataPartsLock & acquired_lock)
{
    if (txn)
        transactions_enabled.store(true);

    auto remove_time = clear_without_timeout ? 0 : time(nullptr);

    for (const DataPartPtr & part : remove)
    {
        if (part->version.creation_csn != Tx::RolledBackCSN)
            MergeTreeTransaction::removeOldPart(shared_from_this(), part, txn);

        if (part->getState() == MergeTreeDataPartState::Active)
        {
            removePartContributionToColumnAndSecondaryIndexSizes(part);
            removePartContributionToDataVolume(part);
        }

        if (part->getState() == MergeTreeDataPartState::Active || clear_without_timeout)
            part->remove_time.store(remove_time, std::memory_order_relaxed);

        if (part->getState() != MergeTreeDataPartState::Outdated)
            modifyPartState(part, MergeTreeDataPartState::Outdated);

        if (isInMemoryPart(part) && getSettings()->in_memory_parts_enable_wal)
            getWriteAheadLog()->dropPart(part->name);
    }

    /// if (removed_active_part)
    ///     resetObjectColumnsFromActiveParts(acquired_lock);
}

void MergeTreeData::removePartsFromWorkingSetImmediatelyAndSetTemporaryState(const DataPartsVector & remove)
{
    auto lock = lockParts();

    for (const auto & part : remove)
    {
        auto it_part = data_parts_by_info.find(part->info);
        if (it_part == data_parts_by_info.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Part {} not found in data_parts", part->getNameWithState());

        assert(part->getState() == MergeTreeDataPartState::PreActive);

        modifyPartState(part, MergeTreeDataPartState::Temporary);
        /// Erase immediately
        data_parts_indexes.erase(it_part);
    }
}

void MergeTreeData::removePartsFromWorkingSet(
        MergeTreeTransaction * txn, const DataPartsVector & remove, bool clear_without_timeout, DataPartsLock * acquired_lock)
{
    auto lock = (acquired_lock) ? DataPartsLock() : lockParts();

    for (const auto & part : remove)
    {
        if (!data_parts_by_info.contains(part->info))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Part {} not found in data_parts", part->getNameWithState());

        part->assertState({DataPartState::PreActive, DataPartState::Active, DataPartState::Outdated});
    }

    removePartsFromWorkingSet(txn, remove, clear_without_timeout, lock);
}


void MergeTreeData::removePartsInRangeFromWorkingSet(MergeTreeTransaction * txn, const MergeTreePartInfo & drop_range, DataPartsLock & lock)
{
    removePartsInRangeFromWorkingSetAndGetPartsToRemoveFromZooKeeper(txn, drop_range, lock);
}

DataPartsVector MergeTreeData::grabActivePartsToRemoveForDropRange(
    MergeTreeTransaction * txn, const MergeTreePartInfo & drop_range, DataPartsLock & lock)
{
    DataPartsVector parts_to_remove;

    if (drop_range.min_block > drop_range.max_block)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid drop range: {}", drop_range.getPartNameForLogs());

    auto partition_range = getVisibleDataPartsVectorInPartition(txn, drop_range.partition_id, &lock);

    for (const DataPartPtr & part : partition_range)
    {
        if (part->info.partition_id != drop_range.partition_id)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected partition_id of part {}. This is a bug.", part->name);

        /// It's a DROP PART and it's already executed by fetching some covering part
        bool is_drop_part = !drop_range.isFakeDropRangePart() && drop_range.min_block;

        if (is_drop_part && (part->info.min_block != drop_range.min_block || part->info.max_block != drop_range.max_block || part->info.getMutationVersion() != drop_range.getMutationVersion()))
        {
            /// Why we check only min and max blocks here without checking merge
            /// level? It's a tricky situation which can happen on a stale
            /// replica. For example, we have parts all_1_1_0, all_2_2_0 and
            /// all_3_3_0. Fast replica assign some merges (OPTIMIZE FINAL or
            /// TTL) all_2_2_0 -> all_2_2_1 -> all_2_2_2. So it has set of parts
            /// all_1_1_0, all_2_2_2 and all_3_3_0. After that it decides to
            /// drop part all_2_2_2. Now set of parts is all_1_1_0 and
            /// all_3_3_0. Now fast replica assign merge all_1_1_0 + all_3_3_0
            /// to all_1_3_1 and finishes it. Slow replica pulls the queue and
            /// have two contradictory tasks -- drop all_2_2_2 and merge/fetch
            /// all_1_3_1. If this replica will fetch all_1_3_1 first and then tries
            /// to drop all_2_2_2 after that it will receive the LOGICAL ERROR.
            /// So here we just check that all_1_3_1 covers blocks from drop
            /// all_2_2_2.
            ///
            bool is_covered_by_min_max_block = part->info.min_block <= drop_range.min_block && part->info.max_block >= drop_range.max_block && part->info.getMutationVersion() >= drop_range.getMutationVersion();
            if (is_covered_by_min_max_block)
            {
                LOG_INFO(log, "Skipping drop range for part {} because covering part {} already exists", drop_range.getPartNameForLogs(), part->name);
                return {};
            }
        }

        if (part->info.min_block < drop_range.min_block)
        {
            if (drop_range.min_block <= part->info.max_block)
            {
                /// Intersect left border
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected merged part {} intersecting drop range {}",
                                part->name, drop_range.getPartNameForLogs());
            }

            continue;
        }

        /// Stop on new parts
        if (part->info.min_block > drop_range.max_block)
            break;

        if (part->info.min_block <= drop_range.max_block && drop_range.max_block < part->info.max_block)
        {
            /// Intersect right border
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected merged part {} intersecting drop range {}",
                            part->name, drop_range.getPartNameForLogs());
        }

        parts_to_remove.emplace_back(part);
    }
    return parts_to_remove;
}

MergeTreeData::PartsToRemoveFromZooKeeper MergeTreeData::removePartsInRangeFromWorkingSetAndGetPartsToRemoveFromZooKeeper(
        MergeTreeTransaction * txn, const MergeTreePartInfo & drop_range, DataPartsLock & lock)
{
#ifndef NDEBUG
    {
        /// All parts (including outdated) must be loaded at this moment.
        std::lock_guard outdated_parts_lock(outdated_data_parts_mutex);
        assert(outdated_unloaded_data_parts.empty());
    }
#endif

    auto parts_to_remove = grabActivePartsToRemoveForDropRange(txn, drop_range, lock);

    bool clear_without_timeout = true;
    /// We a going to remove active parts covered by drop_range without timeout.
    /// Let's also reset timeout for inactive parts
    /// and add these parts to list of parts to remove from ZooKeeper
    auto inactive_parts_to_remove_immediately = getDataPartsVectorInPartitionForInternalUsage(DataPartState::Outdated, drop_range.partition_id, &lock);

    /// FIXME refactor removePartsFromWorkingSet(...), do not remove parts twice
    removePartsFromWorkingSet(txn, parts_to_remove, clear_without_timeout, lock);

    /// Since we can return parts in Deleting state, we have to use a wrapper that restricts access to such parts.
    PartsToRemoveFromZooKeeper parts_to_remove_from_zookeeper;
    for (auto & part : parts_to_remove)
        parts_to_remove_from_zookeeper.emplace_back(std::move(part));

    for (auto & part : inactive_parts_to_remove_immediately)
    {
        if (!drop_range.contains(part->info))
            continue;
        part->remove_time.store(0, std::memory_order_relaxed);
        parts_to_remove_from_zookeeper.emplace_back(std::move(part), /* was_active */ false);
    }

    return parts_to_remove_from_zookeeper;
}

void MergeTreeData::restoreAndActivatePart(const DataPartPtr & part, DataPartsLock * acquired_lock)
{
    auto lock = (acquired_lock) ? DataPartsLock() : lockParts();    //-V1018
    if (part->getState() == DataPartState::Active)
        return;
    addPartContributionToColumnAndSecondaryIndexSizes(part);
    addPartContributionToDataVolume(part);
    modifyPartState(part, DataPartState::Active);
}

void MergeTreeData::forgetPartAndMoveToDetached(const MergeTreeData::DataPartPtr & part_to_detach, const String & prefix, bool restore_covered)
{
    if (prefix.empty())
        LOG_INFO(log, "Renaming {} to {} and forgetting it.", part_to_detach->getDataPartStorage().getPartDirectory(), part_to_detach->name);
    else
        LOG_INFO(log, "Renaming {} to {}_{} and forgetting it.", part_to_detach->getDataPartStorage().getPartDirectory(), prefix, part_to_detach->name);

    auto lock = lockParts();

    auto it_part = data_parts_by_info.find(part_to_detach->info);
    if (it_part == data_parts_by_info.end())
        throw Exception(ErrorCodes::NO_SUCH_DATA_PART, "No such data part {}", part_to_detach->getNameWithState());

    /// What if part_to_detach is a reference to *it_part? Make a new owner just in case.
    /// Important to own part pointer here (not const reference), because it will be removed from data_parts_indexes
    /// few lines below.
    DataPartPtr part = *it_part; // NOLINT


    if (part->getState() == DataPartState::Active)
    {
        removePartContributionToDataVolume(part);
        removePartContributionToColumnAndSecondaryIndexSizes(part);
    }

    modifyPartState(it_part, DataPartState::Deleting);
    asMutableDeletingPart(part)->renameToDetached(prefix);
    data_parts_indexes.erase(it_part);

    if (restore_covered && part->info.level == 0)
    {
        LOG_WARNING(log, "Will not recover parts covered by zero-level part {}", part->name);
        return;
    }

    if (restore_covered)
    {
        Strings restored;
        bool error = false;
        String error_parts;

        Int64 pos = part->info.min_block;

        auto is_appropriate_state = [] (DataPartState state)
        {
            return state == DataPartState::Active || state == DataPartState::Outdated;
        };

        auto update_error = [&] (DataPartIteratorByInfo it)
        {
            error = true;
            error_parts += (*it)->getNameWithState() + " ";
        };

        auto it_middle = data_parts_by_info.lower_bound(part->info);

        /// Restore the leftmost part covered by the part
        if (it_middle != data_parts_by_info.begin())
        {
            auto it = std::prev(it_middle);

            if (part->contains(**it) && is_appropriate_state((*it)->getState()))
            {
                /// Maybe, we must consider part level somehow
                if ((*it)->info.min_block != part->info.min_block)
                    update_error(it);

                if ((*it)->getState() != DataPartState::Active)
                {
                    addPartContributionToColumnAndSecondaryIndexSizes(*it);
                    addPartContributionToDataVolume(*it);
                    modifyPartState(it, DataPartState::Active); // iterator is not invalidated here
                }

                pos = (*it)->info.max_block + 1;
                restored.push_back((*it)->name);
            }
            else
                update_error(it);
        }
        else
            error = true;

        /// Restore "right" parts
        for (auto it = it_middle; it != data_parts_by_info.end() && part->contains(**it); ++it)
        {
            if ((*it)->info.min_block < pos)
                continue;

            if (!is_appropriate_state((*it)->getState()))
            {
                update_error(it);
                continue;
            }

            if ((*it)->info.min_block > pos)
                update_error(it);

            if ((*it)->getState() != DataPartState::Active)
            {
                addPartContributionToColumnAndSecondaryIndexSizes(*it);
                addPartContributionToDataVolume(*it);
                modifyPartState(it, DataPartState::Active);
            }

            pos = (*it)->info.max_block + 1;
            restored.push_back((*it)->name);
        }

        if (pos != part->info.max_block + 1)
            error = true;

        for (const String & name : restored)
        {
            LOG_INFO(log, "Activated part {}", name);
        }

        if (error)
        {
            LOG_WARNING(log, "The set of parts restored in place of {} looks incomplete. "
                             "SELECT queries may observe gaps in data until this replica is synchronized with other replicas.{}",
                        part->name, (error_parts.empty() ? "" : " Suspicious parts: " + error_parts));
        }
    }

    /// if (removed_active_part || restored_active_part)
    ///     resetObjectColumnsFromActiveParts(lock);
}


void MergeTreeData::tryRemovePartImmediately(DataPartPtr && part)
{
    DataPartPtr part_to_delete;
    {
        auto lock = lockParts();

        LOG_TRACE(log, "Trying to immediately remove part {}", part->getNameWithState());

        if (part->getState() != DataPartState::Temporary)
        {
            auto it = data_parts_by_info.find(part->info);
            if (it == data_parts_by_info.end() || (*it).get() != part.get())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Part {} doesn't exist", part->name);

            part.reset();

            if (!((*it)->getState() == DataPartState::Outdated && isSharedPtrUnique(*it)))
                return;

            modifyPartState(it, DataPartState::Deleting);

            part_to_delete = *it;
        }
        else
        {
            part_to_delete = std::move(part);
        }
    }

    try
    {
        asMutableDeletingPart(part_to_delete)->remove();
    }
    catch (...)
    {
        rollbackDeletingParts({part_to_delete});
        throw;
    }

    removePartsFinally({part_to_delete});
    LOG_TRACE(log, "Removed part {}", part_to_delete->name);
}


size_t MergeTreeData::getTotalActiveSizeInBytes() const
{
    return total_active_size_bytes.load(std::memory_order_acquire);
}


size_t MergeTreeData::getTotalActiveSizeInRows() const
{
    return total_active_size_rows.load(std::memory_order_acquire);
}


size_t MergeTreeData::getPartsCount() const
{
    return total_active_size_parts.load(std::memory_order_acquire);
}


std::pair<size_t, size_t> MergeTreeData::getMaxPartsCountAndSizeForPartitionWithState(DataPartState state) const
{
    auto lock = lockParts();

    size_t cur_parts_count = 0;
    size_t cur_parts_size = 0;
    size_t max_parts_count = 0;
    size_t argmax_parts_size = 0;

    const String * cur_partition_id = nullptr;

    for (const auto & part : getDataPartsStateRange(state))
    {
        if (!cur_partition_id || part->info.partition_id != *cur_partition_id)
        {
            cur_partition_id = &part->info.partition_id;
            cur_parts_count = 0;
            cur_parts_size = 0;
        }

        ++cur_parts_count;
        cur_parts_size += part->getBytesOnDisk();

        if (cur_parts_count > max_parts_count)
        {
            max_parts_count = cur_parts_count;
            argmax_parts_size = cur_parts_size;
        }
    }

    return {max_parts_count, argmax_parts_size};
}


std::pair<size_t, size_t> MergeTreeData::getMaxPartsCountAndSizeForPartition() const
{
    return getMaxPartsCountAndSizeForPartitionWithState(DataPartState::Active);
}


size_t MergeTreeData::getMaxOutdatedPartsCountForPartition() const
{
    return getMaxPartsCountAndSizeForPartitionWithState(DataPartState::Outdated).first;
}


std::optional<Int64> MergeTreeData::getMinPartDataVersion() const
{
    auto lock = lockParts();

    std::optional<Int64> result;
    for (const auto & part : getDataPartsStateRange(DataPartState::Active))
    {
        if (!result || *result > part->info.getDataVersion())
            result = part->info.getDataVersion();
    }

    return result;
}


void MergeTreeData::delayInsertOrThrowIfNeeded(Poco::Event * until, const ContextPtr & query_context) const
{
    const auto settings = getSettings();
    const auto & query_settings = query_context->getSettingsRef();
    const size_t parts_count_in_total = getPartsCount();

    /// check if have too many parts in total
    if (parts_count_in_total >= settings->max_parts_in_total)
    {
        ProfileEvents::increment(ProfileEvents::RejectedInserts);
        throw Exception(
            ErrorCodes::TOO_MANY_PARTS,
            "Too many parts ({}) in all partitions in total. This indicates wrong choice of partition key. The threshold can be modified "
            "with 'max_parts_in_total' setting in <merge_tree> element in config.xml or with per-stream setting.",
            parts_count_in_total);
    }

    size_t outdated_parts_over_threshold = 0;
    {
        size_t outdated_parts_count_in_partition = 0;
        if (settings->inactive_parts_to_throw_insert > 0 || settings->inactive_parts_to_delay_insert > 0)
            outdated_parts_count_in_partition = getMaxOutdatedPartsCountForPartition();

        if (settings->inactive_parts_to_throw_insert > 0 && outdated_parts_count_in_partition >= settings->inactive_parts_to_throw_insert)
        {
            ProfileEvents::increment(ProfileEvents::RejectedInserts);
            throw Exception(
                ErrorCodes::TOO_MANY_PARTS,
                "Too many inactive parts ({}). Parts cleaning are processing significantly slower than inserts",
                outdated_parts_count_in_partition);
        }
        if (settings->inactive_parts_to_delay_insert > 0 && outdated_parts_count_in_partition >= settings->inactive_parts_to_delay_insert)
            outdated_parts_over_threshold = outdated_parts_count_in_partition - settings->inactive_parts_to_delay_insert + 1;
    }

    auto [parts_count_in_partition, size_of_partition] = getMaxPartsCountAndSizeForPartition();
    size_t average_part_size = parts_count_in_partition ? size_of_partition / parts_count_in_partition : 0;
    const auto active_parts_to_delay_insert
        = query_settings.parts_to_delay_insert ? query_settings.parts_to_delay_insert : settings->parts_to_delay_insert;
    const auto active_parts_to_throw_insert
        = query_settings.parts_to_throw_insert ? query_settings.parts_to_throw_insert : settings->parts_to_throw_insert;
    size_t active_parts_over_threshold = 0;
    {
        bool parts_are_large_enough_in_average
            = settings->max_avg_part_size_for_too_many_parts && average_part_size > settings->max_avg_part_size_for_too_many_parts;

        if (parts_count_in_partition >= active_parts_to_throw_insert && !parts_are_large_enough_in_average)
        {
            ProfileEvents::increment(ProfileEvents::RejectedInserts);
            throw Exception(
                ErrorCodes::TOO_MANY_PARTS,
                "Too many parts ({} with average size of {}). Merges are processing significantly slower than inserts",
                parts_count_in_partition,
                ReadableSize(average_part_size));
        }
        if (active_parts_to_delay_insert > 0 && parts_count_in_partition >= active_parts_to_delay_insert
            && !parts_are_large_enough_in_average)
            /// if parts_count == parts_to_delay_insert -> we're 1 part over threshold
            active_parts_over_threshold = parts_count_in_partition - active_parts_to_delay_insert + 1;
    }

    /// no need for delay
    if (!active_parts_over_threshold && !outdated_parts_over_threshold)
        return;

    UInt64 delay_milliseconds = 0;
    {
        size_t parts_over_threshold = 0;
        size_t allowed_parts_over_threshold = 1;
        const bool use_active_parts_threshold = (active_parts_over_threshold >= outdated_parts_over_threshold);
        if (use_active_parts_threshold)
        {
            parts_over_threshold = active_parts_over_threshold;
            allowed_parts_over_threshold = active_parts_to_throw_insert - active_parts_to_delay_insert;
        }
        else
        {
            parts_over_threshold = outdated_parts_over_threshold;
            allowed_parts_over_threshold = outdated_parts_over_threshold; /// if throw threshold is not set, will use max delay
            if (settings->inactive_parts_to_throw_insert > 0)
                allowed_parts_over_threshold = settings->inactive_parts_to_throw_insert - settings->inactive_parts_to_delay_insert;
        }

        if (allowed_parts_over_threshold == 0 || parts_over_threshold > allowed_parts_over_threshold) [[unlikely]]
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Incorrect calculation of {} parts over threshold: allowed_parts_over_threshold={}, parts_over_threshold={}",
                (use_active_parts_threshold ? "active" : "inactive"),
                allowed_parts_over_threshold,
                parts_over_threshold);

        const UInt64 max_delay_milliseconds = (settings->max_delay_to_insert > 0 ? settings->max_delay_to_insert * 1000 : 1000);
        double delay_factor = static_cast<double>(parts_over_threshold) / allowed_parts_over_threshold;
        const UInt64 min_delay_milliseconds = settings->min_delay_to_insert_ms;
        delay_milliseconds = std::max(min_delay_milliseconds, static_cast<UInt64>(max_delay_milliseconds * delay_factor));
    }

    ProfileEvents::increment(ProfileEvents::DelayedInserts);
    ProfileEvents::increment(ProfileEvents::DelayedInsertsMilliseconds, delay_milliseconds);

    CurrentMetrics::Increment metric_increment(CurrentMetrics::DelayedInserts);

    LOG_INFO(log, "Delaying inserting block by {} ms. because there are {} parts and their average size is {}",
        delay_milliseconds, parts_count_in_partition, ReadableSize(average_part_size));

    if (until)
        until->tryWait(delay_milliseconds);
    else
        std::this_thread::sleep_for(std::chrono::milliseconds(static_cast<size_t>(delay_milliseconds)));
}


MergeTreeData::DataPartPtr MergeTreeData::getActiveContainingPart(
    const MergeTreePartInfo & part_info, MergeTreeData::DataPartState state, DataPartsLock & /*lock*/) const
{
    auto current_state_parts_range = getDataPartsStateRange(state);

    /// The part can be covered only by the previous or the next one in data_parts.
    auto it = data_parts_by_state_and_info.lower_bound(DataPartStateAndInfo{state, part_info});

    if (it != current_state_parts_range.end())
    {
        if ((*it)->info == part_info)
            return *it;
        if ((*it)->info.contains(part_info))
            return *it;
    }

    if (it != current_state_parts_range.begin())
    {
        --it;
        if ((*it)->info.contains(part_info))
            return *it;
    }

    return nullptr;
}


void MergeTreeData::swapActivePart(MergeTreeData::DataPartPtr part_copy)
{
    auto lock = lockParts();
    for (auto original_active_part : getDataPartsStateRange(DataPartState::Active)) // NOLINT (copy is intended)
    {
        if (part_copy->name == original_active_part->name)
        {
            auto active_part_it = data_parts_by_info.find(original_active_part->info);
            if (active_part_it == data_parts_by_info.end())
                throw Exception(ErrorCodes::NO_SUCH_DATA_PART, "Cannot swap part '{}', no such active part.", part_copy->name);

            /// We do not check allow_remote_fs_zero_copy_replication here because data may be shared
            /// when allow_remote_fs_zero_copy_replication turned on and off again
            original_active_part->force_keep_shared_data = false;

            if (original_active_part->getDataPartStorage().supportZeroCopyReplication() &&
                part_copy->getDataPartStorage().supportZeroCopyReplication() &&
                original_active_part->getDataPartStorage().getUniqueId() == part_copy->getDataPartStorage().getUniqueId())
            {
                /// May be when several volumes use the same S3/HDFS storage
                original_active_part->force_keep_shared_data = true;
            }

            modifyPartState(original_active_part, DataPartState::DeleteOnDestroy);
            data_parts_indexes.erase(active_part_it);

            auto part_it = data_parts_indexes.insert(part_copy).first;
            modifyPartState(part_it, DataPartState::Active);

            ssize_t diff_bytes = part_copy->getBytesOnDisk() - original_active_part->getBytesOnDisk();
            ssize_t diff_rows = part_copy->rows_count - original_active_part->rows_count;
            increaseDataVolume(diff_bytes, diff_rows, /* parts= */ 0);

            /// Move parts are non replicated operations, so we take lock here.
            /// All other locks are taken in StorageReplicatedMergeTree
            lockSharedData(*part_copy);

            asMutableDeletingPart(original_active_part)->writeDeleteOnDestroyMarker();
            return;
        }
    }
    throw Exception(ErrorCodes::NO_SUCH_DATA_PART, "Cannot swap part '{}', no such active part.", part_copy->name);
}


MergeTreeData::DataPartPtr MergeTreeData::getActiveContainingPart(const MergeTreePartInfo & part_info) const
{
    auto lock = lockParts();
    return getActiveContainingPart(part_info, DataPartState::Active, lock);
}

MergeTreeData::DataPartPtr MergeTreeData::getActiveContainingPart(const String & part_name) const
{
    auto part_info = MergeTreePartInfo::fromPartName(part_name, format_version);
    return getActiveContainingPart(part_info);
}

MergeTreeData::DataPartsVector MergeTreeData::getVisibleDataPartsVectorInPartition(ContextPtr local_context, const String & partition_id) const
{
    return getVisibleDataPartsVectorInPartition(local_context->getCurrentTransaction().get(), partition_id);
}


MergeTreeData::DataPartsVector MergeTreeData::getVisibleDataPartsVectorInPartition(
    ContextPtr local_context, const String & partition_id, DataPartsLock & lock) const
{
    return getVisibleDataPartsVectorInPartition(local_context->getCurrentTransaction().get(), partition_id, &lock);
}

MergeTreeData::DataPartsVector MergeTreeData::getVisibleDataPartsVectorInPartition(
    MergeTreeTransaction * txn, const String & partition_id, DataPartsLock * acquired_lock) const
{
    if (txn)
    {
        DataPartStateAndPartitionID active_parts{MergeTreeDataPartState::Active, partition_id};
        DataPartStateAndPartitionID outdated_parts{MergeTreeDataPartState::Outdated, partition_id};
        DataPartsVector res;
        {
            auto lock = (acquired_lock) ? DataPartsLock() : lockParts();
            res.insert(res.end(), data_parts_by_state_and_info.lower_bound(active_parts), data_parts_by_state_and_info.upper_bound(active_parts));
            res.insert(res.end(), data_parts_by_state_and_info.lower_bound(outdated_parts), data_parts_by_state_and_info.upper_bound(outdated_parts));
        }
        filterVisibleDataParts(res, txn->getSnapshot(), txn->tid);
        return res;
    }

    return getDataPartsVectorInPartitionForInternalUsage(MergeTreeDataPartState::Active, partition_id, acquired_lock);
}

MergeTreeData::DataPartsVector MergeTreeData::getDataPartsVectorInPartitionForInternalUsage(const DataPartStates & affordable_states, const String & partition_id, DataPartsLock * acquired_lock) const
{
    auto lock = (acquired_lock) ? DataPartsLock() : lockParts();
    DataPartsVector res;
    for (const auto & state : affordable_states)
    {
        DataPartStateAndPartitionID state_with_partition{state, partition_id};
        res.insert(res.end(), data_parts_by_state_and_info.lower_bound(state_with_partition), data_parts_by_state_and_info.upper_bound(state_with_partition));
    }
    return res;
}

MergeTreeData::DataPartsVector MergeTreeData::getDataPartsVectorInPartitionForInternalUsage(
    const MergeTreeData::DataPartState & state, const String & partition_id, DataPartsLock * acquired_lock) const
{
    DataPartStateAndPartitionID state_with_partition{state, partition_id};

    auto lock = (acquired_lock) ? DataPartsLock() : lockParts();
    return DataPartsVector(
        data_parts_by_state_and_info.lower_bound(state_with_partition),
        data_parts_by_state_and_info.upper_bound(state_with_partition));
}

MergeTreeData::DataPartsVector MergeTreeData::getVisibleDataPartsVectorInPartitions(ContextPtr local_context, const std::unordered_set<String> & partition_ids) const
{
    auto txn = local_context->getCurrentTransaction();
    DataPartsVector res;
    {
        auto lock = lockParts();
        for (const auto & partition_id : partition_ids)
        {
            DataPartStateAndPartitionID active_parts{MergeTreeDataPartState::Active, partition_id};
            insertAtEnd(
                res,
                DataPartsVector(
                    data_parts_by_state_and_info.lower_bound(active_parts),
                    data_parts_by_state_and_info.upper_bound(active_parts)));

            if (txn)
            {
                DataPartStateAndPartitionID outdated_parts{MergeTreeDataPartState::Active, partition_id};

                insertAtEnd(
                    res,
                    DataPartsVector(
                        data_parts_by_state_and_info.lower_bound(outdated_parts),
                        data_parts_by_state_and_info.upper_bound(outdated_parts)));
            }
        }
    }

    if (txn)
        filterVisibleDataParts(res, txn->getSnapshot(), txn->tid);

    return res;
}

MergeTreeData::DataPartPtr MergeTreeData::getPartIfExists(const MergeTreePartInfo & part_info, const MergeTreeData::DataPartStates & valid_states)
{
    auto lock = lockParts();
    return getPartIfExistsUnlocked(part_info, valid_states, lock);
}

MergeTreeData::DataPartPtr MergeTreeData::getPartIfExists(const String & part_name, const MergeTreeData::DataPartStates & valid_states)
{
    auto lock = lockParts();
    return getPartIfExistsUnlocked(part_name, valid_states, lock);
}

MergeTreeData::DataPartPtr MergeTreeData::getPartIfExistsUnlocked(const String & part_name, const DataPartStates & valid_states, DataPartsLock & acquired_lock)
{
    return getPartIfExistsUnlocked(MergeTreePartInfo::fromPartName(part_name, format_version), valid_states, acquired_lock);
}

MergeTreeData::DataPartPtr MergeTreeData::getPartIfExistsUnlocked(const MergeTreePartInfo & part_info, const DataPartStates & valid_states, DataPartsLock & /* acquired_lock */)
{
    auto it = data_parts_by_info.find(part_info);
    if (it == data_parts_by_info.end())
        return nullptr;

    for (auto state : valid_states)
        if ((*it)->getState() == state)
            return *it;

    return nullptr;
}

static void loadPartAndFixMetadataImpl(MergeTreeData::MutableDataPartPtr part, ContextPtr local_context, int32_t metadata_version, bool sync)
{
    /// Remove metadata version file and take it from table.
    /// Currently we cannot attach parts with different schema, so
    /// we can assume that it's equal to table's current schema.
    part->removeMetadataVersion();
    {
        auto out_metadata = part->getDataPartStorage().writeFile(IMergeTreeDataPart::METADATA_VERSION_FILE_NAME, 4096, local_context->getWriteSettings());
        writeText(metadata_version, *out_metadata);
        out_metadata->finalize();
        if (sync)
            out_metadata->sync();
    }

    part->loadColumnsChecksumsIndexes(false, true);
    part->modification_time = part->getDataPartStorage().getLastModified().epochTime();
    part->removeDeleteOnDestroyMarker();
    part->removeVersionMetadata();
}

void MergeTreeData::calculateColumnAndSecondaryIndexSizesImpl()
{
    column_sizes.clear();

    /// Take into account only committed parts
    auto committed_parts_range = getDataPartsStateRange(DataPartState::Active);
    for (const auto & part : committed_parts_range)
        addPartContributionToColumnAndSecondaryIndexSizes(part);
}

void MergeTreeData::addPartContributionToColumnAndSecondaryIndexSizes(const DataPartPtr & part)
{
    for (const auto & column : part->getColumns())
    {
        ColumnSize & total_column_size = column_sizes[column.name];
        ColumnSize part_column_size = part->getColumnSize(column.name);
        total_column_size.add(part_column_size);
    }

    auto indexes_descriptions = getInMemoryMetadataPtr()->secondary_indices;
    for (const auto & index : indexes_descriptions)
    {
        IndexSize & total_secondary_index_size = secondary_index_sizes[index.name];
        IndexSize part_index_size = part->getSecondaryIndexSize(index.name);
        total_secondary_index_size.add(part_index_size);
    }
}

void MergeTreeData::removePartContributionToColumnAndSecondaryIndexSizes(const DataPartPtr & part)
{
    for (const auto & column : part->getColumns())
    {
        ColumnSize & total_column_size = column_sizes[column.name];
        ColumnSize part_column_size = part->getColumnSize(column.name);

        auto log_subtract = [&](size_t & from, size_t value, const char * field)
        {
            if (value > from)
                LOG_ERROR(log, "Possibly incorrect column size subtraction: {} - {} = {}, column: {}, field: {}",
                    from, value, from - value, column.name, field);

            from -= value;
        };

        log_subtract(total_column_size.data_compressed, part_column_size.data_compressed, ".data_compressed");
        log_subtract(total_column_size.data_uncompressed, part_column_size.data_uncompressed, ".data_uncompressed");
        log_subtract(total_column_size.marks, part_column_size.marks, ".marks");
    }

    auto indexes_descriptions = getInMemoryMetadataPtr()->secondary_indices;
    for (const auto & index : indexes_descriptions)
    {
        IndexSize & total_secondary_index_size = secondary_index_sizes[index.name];
        IndexSize part_secondary_index_size = part->getSecondaryIndexSize(index.name);

        auto log_subtract = [&](size_t & from, size_t value, const char * field)
        {
            if (value > from)
                LOG_ERROR(log, "Possibly incorrect index size subtraction: {} - {} = {}, index: {}, field: {}",
                    from, value, from - value, index.name, field);

            from -= value;
        };

        log_subtract(total_secondary_index_size.data_compressed, part_secondary_index_size.data_compressed, ".data_compressed");
        log_subtract(total_secondary_index_size.data_uncompressed, part_secondary_index_size.data_uncompressed, ".data_uncompressed");
        log_subtract(total_secondary_index_size.marks, part_secondary_index_size.marks, ".marks");
    }
}

void MergeTreeData::checkAlterPartitionIsPossible(
    const PartitionCommands & commands, const StorageMetadataPtr & /*metadata_snapshot*/, const Settings & settings) const
{
    for (const auto & disk : getDisks())
        if (!disk->supportsHardLinks())
            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED, "ALTER TABLE PARTITION is not supported for immutable disk '{}'", disk->getName());

    for (const auto & command : commands)
    {
        if (command.type == PartitionCommand::DROP_DETACHED_PARTITION
            && !settings.allow_drop_detached)
            throw DB::Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                                "Cannot execute query: DROP DETACHED PART "
                                "is disabled (see allow_drop_detached setting)");

        if (command.partition && command.type != PartitionCommand::DROP_DETACHED_PARTITION)
        {
            if (command.part)
            {
                auto part_name = command.partition->as<ASTLiteral &>().value.safeGet<String>();
                /// We are able to parse it
                MergeTreePartInfo::fromPartName(part_name, format_version);
            }
            else
            {
                /// We are able to parse it
                getPartitionIDFromQuery(command.partition, getContext());
            }
        }
    }
}

void MergeTreeData::checkPartitionCanBeDropped(const ASTPtr & partition, ContextPtr local_context)
{
    DataPartsVector parts_to_remove;
    const auto * partition_ast = partition->as<ASTPartition>();
    if (partition_ast && partition_ast->all)
        parts_to_remove = getVisibleDataPartsVector(local_context);
    else
    {
        const String partition_id = getPartitionIDFromQuery(partition, local_context);
        parts_to_remove = getVisibleDataPartsVectorInPartition(local_context, partition_id);
    }
    UInt64 partition_size = 0;

    for (const auto & part : parts_to_remove)
        partition_size += part->getBytesOnDisk();

    auto table_id = getStorageID();
    getContext()->checkPartitionCanBeDropped(table_id.database_name, table_id.table_name, partition_size);
}

void MergeTreeData::checkPartCanBeDropped(const String & part_name)
{
    auto part = getPartIfExists(part_name, {MergeTreeDataPartState::Active});
    if (!part)
        throw Exception(ErrorCodes::NO_SUCH_DATA_PART, "No part {} in committed state", part_name);

    auto table_id = getStorageID();
    getContext()->checkPartitionCanBeDropped(table_id.database_name, table_id.table_name, part->getBytesOnDisk());
}

void MergeTreeData::movePartitionToDisk(const ASTPtr & partition, const String & name, bool moving_part, ContextPtr local_context)
{
    String partition_id;

    if (moving_part)
        partition_id = partition->as<ASTLiteral &>().value.safeGet<String>();
    else
        partition_id = getPartitionIDFromQuery(partition, local_context);

    DataPartsVector parts;
    if (moving_part)
    {
        auto part_info = MergeTreePartInfo::fromPartName(partition_id, format_version);
        parts.push_back(getActiveContainingPart(part_info));
        if (!parts.back() || parts.back()->name != part_info.getPartNameAndCheckFormat(format_version))
            throw Exception(ErrorCodes::NO_SUCH_DATA_PART, "Part {} is not exists or not active", partition_id);
    }
    else
        parts = getVisibleDataPartsVectorInPartition(local_context, partition_id);

    auto disk = getStoragePolicy()->getDiskByName(name);
    std::erase_if(parts, [&](auto part_ptr)
        {
            return part_ptr->getDataPartStorage().getDiskName() == disk->getName();
        });

    if (parts.empty())
    {
        String no_parts_to_move_message;
        if (moving_part)
            throw Exception(ErrorCodes::UNKNOWN_DISK, "Part '{}' is already on disk '{}'", partition_id, disk->getName());
        else
            throw Exception(ErrorCodes::UNKNOWN_DISK, "All parts of partition '{}' are already on disk '{}'", partition_id, disk->getName());
    }

    if (!movePartsToSpace(parts, std::static_pointer_cast<Space>(disk), local_context->getReadSettings(), local_context->getWriteSettings()))
        throw Exception(ErrorCodes::ABORTED, "Cannot move parts because moves are manually disabled");
}


void MergeTreeData::movePartitionToVolume(const ASTPtr & partition, const String & name, bool moving_part, ContextPtr local_context)
{
    String partition_id;

    if (moving_part)
        partition_id = partition->as<ASTLiteral &>().value.safeGet<String>();
    else
        partition_id = getPartitionIDFromQuery(partition, local_context);

    DataPartsVector parts;
    if (moving_part)
    {
        auto part_info = MergeTreePartInfo::fromPartName(partition_id, format_version);
        parts.emplace_back(getActiveContainingPart(part_info));
        if (!parts.back() || parts.back()->name != part_info.getPartNameAndCheckFormat(format_version))
            throw Exception(ErrorCodes::NO_SUCH_DATA_PART, "Part {} is not exists or not active", partition_id);
    }
    else
        parts = getVisibleDataPartsVectorInPartition(local_context, partition_id);

    auto volume = getStoragePolicy()->getVolumeByName(name);
    if (!volume)
        throw Exception(ErrorCodes::UNKNOWN_DISK, "Volume {} does not exists on policy {}", name, getStoragePolicy()->getName());

    if (parts.empty())
        throw Exception(ErrorCodes::NO_SUCH_DATA_PART, "Nothing to move (check that the partition exists).");

    std::erase_if(parts, [&](auto part_ptr)
        {
            for (const auto & disk : volume->getDisks())
            {
                if (part_ptr->getDataPartStorage().getDiskName() == disk->getName())
                {
                    return true;
                }
            }
            return false;
        });

    if (parts.empty())
    {
        String no_parts_to_move_message;
        if (moving_part)
            throw Exception(ErrorCodes::UNKNOWN_DISK, "Part '{}' is already on volume '{}'", partition_id, volume->getName());
        else
            throw Exception(ErrorCodes::UNKNOWN_DISK, "All parts of partition '{}' are already on volume '{}'", partition_id, volume->getName());
    }

    if (!movePartsToSpace(parts, std::static_pointer_cast<Space>(volume), local_context->getReadSettings(), local_context->getWriteSettings()))
        throw Exception(ErrorCodes::ABORTED, "Cannot move parts because moves are manually disabled");
}

void MergeTreeData::movePartitionToShard(const ASTPtr & /*partition*/, bool /*move_part*/, const String & /*to*/, ContextPtr /*query_context*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "MOVE PARTITION TO SHARD is not supported by storage {}", getName());
}

void MergeTreeData::fetchPartition(
    const ASTPtr & /*partition*/,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    const String & /*from*/,
    bool /*fetch_part*/,
    ContextPtr /*query_context*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "FETCH PARTITION is not supported by storage {}", getName());
}

Pipe MergeTreeData::alterPartition(
    const StorageMetadataPtr & metadata_snapshot,
    const PartitionCommands & commands,
    ContextPtr query_context)
{
    /// Wait for loading of outdated parts
    /// because partition commands (DROP, MOVE, etc.)
    /// must be applied to all parts on disk.
    waitForOutdatedPartsToBeLoaded();

    PartitionCommandsResultInfo result;
    for (const PartitionCommand & command : commands)
    {
        PartitionCommandsResultInfo current_command_results;
        switch (command.type)
        {
            case PartitionCommand::DROP_PARTITION:
            {
                if (command.part)
                {
                    auto part_name = command.partition->as<ASTLiteral &>().value.safeGet<String>();
                    checkPartCanBeDropped(part_name);
                    dropPart(part_name, command.detach, query_context);
                }
                else
                {
                    checkPartitionCanBeDropped(command.partition, query_context);
                    dropPartition(command.partition, command.detach, query_context);
                }
            }
            break;

            case PartitionCommand::DROP_DETACHED_PARTITION:
                dropDetached(command.partition, command.part, query_context);
                break;

            case PartitionCommand::ATTACH_PARTITION:
                current_command_results = attachPartition(command.partition, metadata_snapshot, command.part, query_context);
                break;
            case PartitionCommand::MOVE_PARTITION:
            {
                switch (*command.move_destination_type)
                {
                    case PartitionCommand::MoveDestinationType::DISK:
                        movePartitionToDisk(command.partition, command.move_destination_name, command.part, query_context);
                        break;

                    case PartitionCommand::MoveDestinationType::VOLUME:
                        movePartitionToVolume(command.partition, command.move_destination_name, command.part, query_context);
                        break;

                    case PartitionCommand::MoveDestinationType::TABLE:
                    {
                        String dest_database = query_context->resolveDatabase(command.to_database);
                        auto dest_storage = DatabaseCatalog::instance().getTable({dest_database, command.to_table}, query_context);

                        auto * dest_storage_merge_tree = dynamic_cast<MergeTreeData *>(dest_storage.get());
                        if (!dest_storage_merge_tree)
                            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                                "Cannot move partition from table {} to table {} with storage {}",
                                getStorageID().getNameForLogs(), dest_storage->getStorageID().getNameForLogs(), dest_storage->getName());

                        dest_storage_merge_tree->waitForOutdatedPartsToBeLoaded();
                        movePartitionToTable(dest_storage, command.partition, query_context);
                    }
                    break;

                    case PartitionCommand::MoveDestinationType::SHARD:
                    {
                        if (!getSettings()->part_moves_between_shards_enable)
                            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                                            "Moving parts between shards is experimental and work in progress"
                                            ", see part_moves_between_shards_enable setting");
                        movePartitionToShard(command.partition, command.part, command.move_destination_name, query_context);
                    }
                    break;
                }
            }
            break;

            case PartitionCommand::REPLACE_PARTITION:
            {
                if (command.replace)
                    checkPartitionCanBeDropped(command.partition, query_context);

                String from_database = query_context->resolveDatabase(command.from_database);
                auto from_storage = DatabaseCatalog::instance().getTable({from_database, command.from_table}, query_context);

                auto * from_storage_merge_tree = dynamic_cast<MergeTreeData *>(from_storage.get());
                if (!from_storage_merge_tree)
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Cannot replace partition from table {} with storage {} to table {}",
                        from_storage->getStorageID().getNameForLogs(), from_storage->getName(), getStorageID().getNameForLogs());

                from_storage_merge_tree->waitForOutdatedPartsToBeLoaded();
                replacePartitionFrom(from_storage, command.partition, command.replace, query_context);
            }
            break;

            case PartitionCommand::FETCH_PARTITION:
                fetchPartition(command.partition, metadata_snapshot, command.from_zookeeper_path, command.part, query_context);
                break;

            case PartitionCommand::FREEZE_PARTITION:
            {
                auto lock = lockForShare(query_context->getCurrentQueryId(), query_context->getSettingsRef().lock_acquire_timeout);
                current_command_results = freezePartition(command.partition, metadata_snapshot, command.with_name, query_context, lock);
            }
            break;

            case PartitionCommand::FREEZE_ALL_PARTITIONS:
            {
                auto lock = lockForShare(query_context->getCurrentQueryId(), query_context->getSettingsRef().lock_acquire_timeout);
                current_command_results = freezeAll(command.with_name, metadata_snapshot, query_context, lock);
            }
            break;

            case PartitionCommand::UNFREEZE_PARTITION:
            {
                auto lock = lockForShare(query_context->getCurrentQueryId(), query_context->getSettingsRef().lock_acquire_timeout);
                current_command_results = unfreezePartition(command.partition, command.with_name, query_context, lock);
            }
            break;

            case PartitionCommand::UNFREEZE_ALL_PARTITIONS:
            {
                auto lock = lockForShare(query_context->getCurrentQueryId(), query_context->getSettingsRef().lock_acquire_timeout);
                current_command_results = unfreezeAll(command.with_name, query_context, lock);
            }

            break;

            default:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Uninitialized partition command");
        }
        for (auto & command_result : current_command_results)
            command_result.command_type = command.typeToString();
        result.insert(result.end(), current_command_results.begin(), current_command_results.end());
    }

    if (query_context->getSettingsRef().alter_partition_verbose_result)
        return convertCommandsResultToSource(result);

    return {};
}



BackupEntries MergeTreeData::backup(const ASTs & partitions, ContextPtr local_context)
{
    DataPartsVector data_parts;
    if (partitions.empty())
        data_parts = getVisibleDataPartsVector(local_context);
    else
        data_parts = getVisibleDataPartsVectorInPartitions(local_context, getPartitionIDsFromQuery(partitions, local_context));
    return backupDataParts(data_parts, local_context);
}


BackupEntries MergeTreeData::backupDataParts(const DataPartsVector & data_parts, const ContextPtr & local_context)
{
    BackupEntries backup_entries;
    std::map<DiskPtr, std::shared_ptr<TemporaryFileOnDisk>> temp_dirs;
    /// proton : FIXME, merge backup / restore PRs
    fs::path data_path_in_backup_fs = "/";

    ReadSettings read_settings = local_context->getBackupReadSettings();

    for (const auto & part : data_parts)
        part->getDataPartStorage().backup(read_settings, temp_dirs, part->checksums, part->getFileNamesWithoutChecksums(), backup_entries);

    /// TODO: try to write better code later.
    for (auto & entry : backup_entries)
        entry.first = data_path_in_backup_fs / entry.first;

    return backup_entries;
}


RestoreDataTasks MergeTreeData::restoreDataPartsFromBackup(const BackupPtr & backup, const String & data_path_in_backup,
                                                           const std::unordered_set<String> & partition_ids,
                                                           SimpleIncrement * increment)
{
    RestoreDataTasks restore_tasks;

    Strings part_names = backup->listFiles(data_path_in_backup);
    for (const String & part_name : part_names)
    {
        auto part_info = MergeTreePartInfo::tryParsePartName(part_name, format_version);

        if (!part_info)
            continue;

        if (!partition_ids.empty() && !partition_ids.contains(part_info->partition_id))
            continue;

        UInt64 total_size_of_part = 0;
        Strings filenames = backup->listFiles(data_path_in_backup + part_name + "/", "");
        for (const String & filename : filenames)
            total_size_of_part += backup->getFileSize(data_path_in_backup + part_name + "/" + filename);

        std::shared_ptr<IReservation> reservation = getStoragePolicy()->reserveAndCheck(total_size_of_part);

        auto restore_task = [this,
                             backup,
                             data_path_in_backup,
                             part_name,
                             my_part_info = std::move(part_info),
                             my_filenames = std::move(filenames),
                             reservation,
                             increment]()
        {
            auto disk = reservation->getDisk();

            auto temp_part_dir_owner = std::make_shared<TemporaryFileOnDisk>(disk, relative_data_path + "restoring_" + part_name + "_");
            String temp_part_dir = temp_part_dir_owner->getPath();
            disk->createDirectories(temp_part_dir);

            assert(temp_part_dir.starts_with(relative_data_path));
            String relative_temp_part_dir = temp_part_dir.substr(relative_data_path.size());

            for (const String & filename : my_filenames)
            {
                auto backup_entry = backup->readFile(fs::path(data_path_in_backup) / part_name / filename);
                auto read_buffer = backup_entry->getReadBuffer();
                auto write_buffer = disk->writeFile(fs::path(temp_part_dir) / filename);
                copyData(*read_buffer, *write_buffer);
                write_buffer->finalize();
            }

            auto single_disk_volume = std::make_shared<SingleDiskVolume>(disk->getName(), disk, 0);
            auto data_part_storage = std::make_shared<DataPartStorageOnDiskFull>(single_disk_volume, relative_data_path, relative_temp_part_dir);
            auto part = getDataPartBuilder(part_name, single_disk_volume, part_name)
                .withPartInfo(*my_part_info)
                .withPartFormatFromDisk()
                .build();
            /// TODO Transactions: Decide what to do with version metadata (if any). Let's just remove it for now.
            disk->removeFileIfExists(fs::path(temp_part_dir) / IMergeTreeDataPart::TXN_VERSION_METADATA_FILE_NAME);
            part->version.setCreationTID(Tx::PrehistoricTID, nullptr);
            part->loadColumnsChecksumsIndexes(false, true);
            (void) increment;
            /// FIXME, proton, merge backup / restore
            /// storage->renameTempPartAndAdd(part, NO_TRANSACTION_RAW, increment);
            /// renameTempPartAndAdd(part, NO_TRANSACTION_RAW, increment);
        };

        restore_tasks.emplace_back(std::move(restore_task));
    }

    return restore_tasks;
}


String MergeTreeData::getPartitionIDFromQuery(const ASTPtr & ast, ContextPtr local_context, DataPartsLock * acquired_lock) const
{
    const auto & partition_ast = ast->as<ASTPartition &>();

    if (partition_ast.all)
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Only Support DETACH PARTITION ALL currently");

    if (!partition_ast.value)
    {
        MergeTreePartInfo::validatePartitionID(partition_ast.id, format_version);
        return partition_ast.id;
    }

    if (format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
    {
        /// Month-partitioning specific - partition ID can be passed in the partition value.
        const auto * partition_lit = partition_ast.value->as<ASTLiteral>();
        if (partition_lit && partition_lit->value.getType() == Field::Types::String)
        {
            String partition_id = partition_lit->value.get<String>();
            MergeTreePartInfo::validatePartitionID(partition_id, format_version);
            return partition_id;
        }
    }

    /// Re-parse partition key fields using the information about expected field types.
    auto metadata_snapshot = getInMemoryMetadataPtr();
    const Block & key_sample_block = metadata_snapshot->getPartitionKey().sample_block;
    size_t fields_count = key_sample_block.columns();
    if (partition_ast.fields_count != fields_count)
        throw Exception(ErrorCodes::INVALID_PARTITION_VALUE,
                        "Wrong number of fields in the partition expression: {}, must be: {}",
                        partition_ast.fields_count, fields_count);

    Row partition_row(fields_count);
    if (fields_count == 0)
    {
        /// Function tuple(...) requires at least one argument, so empty key is a special case
        assert(!partition_ast.fields_count);
        assert(typeid_cast<ASTFunction *>(partition_ast.value.get()));
        assert(partition_ast.value->as<ASTFunction>()->name == "tuple_cast");
        assert(partition_ast.value->as<ASTFunction>()->arguments);
        auto args = partition_ast.value->as<ASTFunction>()->arguments;
        if (!args)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected at least one argument in partition AST");
        bool empty_tuple = partition_ast.value->as<ASTFunction>()->arguments->children.empty();
        if (!empty_tuple)
            throw Exception(ErrorCodes::INVALID_PARTITION_VALUE, "Partition key is empty, expected 'tuple()' as partition key");
    }
    else if (fields_count == 1)
    {
        ASTPtr partition_value_ast = partition_ast.value;
        if (auto * tuple = partition_value_ast->as<ASTFunction>())
        {
            assert(tuple->name == "tuple_cast");
            assert(tuple->arguments);
            assert(tuple->arguments->children.size() == 1);
            partition_value_ast = tuple->arguments->children[0];
        }
        /// Simple partition key, need to evaluate and cast
        Field partition_key_value = evaluateConstantExpression(partition_value_ast, local_context).first;
        partition_row[0] = convertFieldToTypeOrThrow(partition_key_value, *key_sample_block.getByPosition(0).type);
    }
    else
    {
        /// Complex key, need to evaluate, untuple and cast
        Field partition_key_value = evaluateConstantExpression(partition_ast.value, local_context).first;
        if (partition_key_value.getType() != Field::Types::Tuple)
            throw Exception(ErrorCodes::INVALID_PARTITION_VALUE,
                            "Expected tuple for complex partition key, got {}", partition_key_value.getTypeName());

        const Tuple & tuple = partition_key_value.get<Tuple>();
        if (tuple.size() != fields_count)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Wrong number of fields in the partition expression: {}, must be: {}", tuple.size(), fields_count);

        for (size_t i = 0; i < fields_count; ++i)
            partition_row[i] = convertFieldToTypeOrThrow(tuple[i], *key_sample_block.getByPosition(i).type);
    }

    MergeTreePartition partition(std::move(partition_row));
    String partition_id = partition.getID(*this);

    {
        auto data_parts_lock = (acquired_lock) ? DataPartsLock() : lockParts();
        DataPartPtr existing_part_in_partition = getAnyPartInPartition(partition_id, data_parts_lock);
        if (existing_part_in_partition && existing_part_in_partition->partition.value != partition.value)
        {
            auto partition_str = partition.serializeToString(existing_part_in_partition->getMetadataSnapshot());
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Parsed partition value: {} "
                            "doesn't match partition value for an existing part with the same partition ID: {}",
                            partition_str, existing_part_in_partition->name);
        }
    }

    return partition_id;
}

DataPartsVector MergeTreeData::getVisibleDataPartsVector(ContextPtr local_context) const
{
    return getVisibleDataPartsVector(local_context->getCurrentTransaction());
}

DataPartsVector MergeTreeData::getVisibleDataPartsVectorUnlocked(ContextPtr local_context, const DataPartsLock & lock) const
{
    DataPartsVector res;
    if (const auto * txn = local_context->getCurrentTransaction().get())
    {
        res = getDataPartsVectorForInternalUsage({DataPartState::Active, DataPartState::Outdated}, lock);
        filterVisibleDataParts(res, txn->getSnapshot(), txn->tid);
    }
    else
    {
        res = getDataPartsVectorForInternalUsage({DataPartState::Active}, lock);
    }
    return res;
}

MergeTreeData::DataPartsVector MergeTreeData::getVisibleDataPartsVector(const MergeTreeTransactionPtr & txn) const
{
    DataPartsVector res;
    if (txn)
    {
        res = getDataPartsVectorForInternalUsage({DataPartState::Active, DataPartState::Outdated});
        filterVisibleDataParts(res, txn->getSnapshot(), txn->tid);
    }
    else
    {
        res = getDataPartsVectorForInternalUsage();
    }
    return res;
}

MergeTreeData::DataPartsVector MergeTreeData::getVisibleDataPartsVector(CSN snapshot_version, TransactionID current_tid) const
{
    auto res = getDataPartsVectorForInternalUsage({DataPartState::Active, DataPartState::Outdated});
    filterVisibleDataParts(res, snapshot_version, current_tid);
    return res;
}

void MergeTreeData::filterVisibleDataParts(DataPartsVector & maybe_visible_parts, CSN snapshot_version, TransactionID current_tid) const
{
    [[maybe_unused]] size_t total_size = maybe_visible_parts.size();

    auto need_remove_pred = [snapshot_version, &current_tid] (const DataPartPtr & part) -> bool
    {
        return !part->version.isVisible(snapshot_version, current_tid);
    };

    std::erase_if(maybe_visible_parts, need_remove_pred);
    [[maybe_unused]] size_t visible_size = maybe_visible_parts.size();

    LOG_TEST(log, "Got {} parts (of {}) visible in snapshot {} (TID {}): {}",
             visible_size, total_size, snapshot_version, current_tid, fmt::join(getPartsNames(maybe_visible_parts), ", "));
}


std::unordered_set<String> MergeTreeData::getPartitionIDsFromQuery(const ASTs & asts, ContextPtr local_context) const
{
    std::unordered_set<String> partition_ids;
    for (const auto & ast : asts)
        partition_ids.emplace(getPartitionIDFromQuery(ast, local_context));
    return partition_ids;
}

std::set<String> MergeTreeData::getPartitionIdsAffectedByCommands(
    const MutationCommands & commands, ContextPtr query_context) const
{
    std::set<String> affected_partition_ids;

    for (const auto & command : commands)
    {
        if (!command.partition)
        {
            affected_partition_ids.clear();
            break;
        }

        affected_partition_ids.insert(
            getPartitionIDFromQuery(command.partition, query_context)
        );
    }

    return affected_partition_ids;
}


MergeTreeData::DataPartsVector MergeTreeData::getDataPartsVectorForInternalUsage(
    const DataPartStates & affordable_states,
    const DataPartsLock & /*lock*/,
    DataPartStateVector * out_states,
    bool require_projection_parts) const
{
    DataPartsVector res;
    DataPartsVector buf;

    for (auto state : affordable_states)
    {
        auto range = getDataPartsStateRange(state);

        if (require_projection_parts)
        {
            for (const auto & part : range)
            {
                for (const auto & [_, projection_part] : part->getProjectionParts())
                    res.push_back(projection_part);
            }
        }
        else
        {
            std::swap(buf, res);
            res.clear();
            std::merge(range.begin(), range.end(), buf.begin(), buf.end(), std::back_inserter(res), LessDataPart()); //-V783
        }
    }

    if (out_states != nullptr)
    {
        out_states->resize(res.size());
        if (require_projection_parts)
        {
            for (size_t i = 0; i < res.size(); ++i)
                (*out_states)[i] = res[i]->getParentPart()->getState();
        }
        else
        {
            for (size_t i = 0; i < res.size(); ++i)
                (*out_states)[i] = res[i]->getState();
        }
    }

    return res;
}

MergeTreeData::DataPartsVector MergeTreeData::getDataPartsVectorForInternalUsage(
    const DataPartStates & affordable_states,
    DataPartStateVector * out_states,
    bool require_projection_parts) const
{
    auto lock = lockParts();
    return getDataPartsVectorForInternalUsage(affordable_states, lock, out_states, require_projection_parts);
}

MergeTreeData::DataPartsVector
MergeTreeData::getAllDataPartsVector(MergeTreeData::DataPartStateVector * out_states, bool require_projection_parts) const
{
    DataPartsVector res;
    if (require_projection_parts)
    {
        auto lock = lockParts();
        for (const auto & part : data_parts_by_info)
        {
            for (const auto & [p_name, projection_part] : part->getProjectionParts())
                res.push_back(projection_part);
        }

        if (out_states != nullptr)
        {
            out_states->resize(res.size());
            for (size_t i = 0; i < res.size(); ++i)
                (*out_states)[i] = res[i]->getParentPart()->getState();
        }
    }
    else
    {
        auto lock = lockParts();
        res.assign(data_parts_by_info.begin(), data_parts_by_info.end());

        if (out_states != nullptr)
        {
            out_states->resize(res.size());
            for (size_t i = 0; i < res.size(); ++i)
                (*out_states)[i] = res[i]->getState();
        }
    }

    return res;
}

bool MergeTreeData::supportsLightweightDelete() const
{
    auto lock = lockParts();
    for (const auto & part : data_parts_by_info)
    {
        if (part->getState() == MergeTreeDataPartState::Outdated
            || part->getState() == MergeTreeDataPartState::Deleting)
            continue;

        if (!part->supportLightweightDeleteMutate())
            return false;
    }
    return true;
}

DetachedPartsInfo MergeTreeData::getDetachedParts() const
{
    DetachedPartsInfo res;

    for (const auto & disk : getDisks())
    {
        String detached_path = fs::path(relative_data_path) / MergeTreeData::DETACHED_DIR_NAME;

        /// Note: we don't care about TOCTOU issue here.
        if (disk->exists(detached_path))
        {
            for (auto it = disk->iterateDirectory(detached_path); it->isValid(); it->next())
            {
                res.push_back(DetachedPartInfo::parseDetachedPartName(disk, it->name(), format_version));
            }
        }
    }
    return res;
}

void MergeTreeData::validateDetachedPartName(const String & name)
{
    if (name.find('/') != std::string::npos || name == "." || name == "..")
        throw DB::Exception(ErrorCodes::INCORRECT_FILE_NAME, "Invalid part name '{}'", name);

    if (startsWith(name, "attaching_") || startsWith(name, "deleting_"))
        throw DB::Exception(ErrorCodes::BAD_DATA_PART_NAME, "Cannot drop part {}: "
                            "most likely it is used by another DROP or ATTACH query.", name);
}

void MergeTreeData::dropDetached(const ASTPtr & partition, bool part, ContextPtr local_context)
{
    PartsTemporaryRename renamed_parts(*this, "detached/");

    if (part)
    {
        String part_name = partition->as<ASTLiteral &>().value.safeGet<String>();
        validateDetachedPartName(part_name);
        auto disk = getDiskForDetachedPart(part_name);
        renamed_parts.addPart(part_name, "deleting_" + part_name, disk);
    }
    else
    {
        String partition_id = getPartitionIDFromQuery(partition, local_context);
        DetachedPartsInfo detached_parts = getDetachedParts();
        for (const auto & part_info : detached_parts)
            if (part_info.valid_name && part_info.partition_id == partition_id
                && part_info.prefix != "attaching" && part_info.prefix != "deleting")
                renamed_parts.addPart(part_info.dir_name, "deleting_" + part_info.dir_name, part_info.disk);
    }

    LOG_DEBUG(log, "Will drop {} detached parts.", renamed_parts.old_and_new_names.size());

    renamed_parts.tryRenameAll();

    for (auto & [old_name, new_name, disk] : renamed_parts.old_and_new_names)
    {
        bool keep_shared = removeDetachedPart(disk, fs::path(relative_data_path) / "detached" / new_name / "", old_name, false);
        LOG_DEBUG(log, "Dropped detached part {}, keep shared data: {}", old_name, keep_shared);
        old_name.clear();
    }
}

MergeTreeData::MutableDataPartsVector MergeTreeData::tryLoadPartsToAttach(const ASTPtr & partition, bool attach_part,
        ContextPtr local_context, PartsTemporaryRename & renamed_parts)
{
    const String source_dir = "detached/";

    std::map<String, DiskPtr> name_to_disk;

    /// Let's compose a list of parts that should be added.
    if (attach_part)
    {
        const String part_id = partition->as<ASTLiteral &>().value.safeGet<String>();
        validateDetachedPartName(part_id);
        auto disk = getDiskForDetachedPart(part_id);
        renamed_parts.addPart(part_id, "attaching_" + part_id, disk);

        if (MergeTreePartInfo::tryParsePartName(part_id, format_version))
            name_to_disk[part_id] = getDiskForDetachedPart(part_id);
    }
    else
    {
        String partition_id = getPartitionIDFromQuery(partition, local_context);
        LOG_DEBUG(log, "Looking for parts for partition {} in {}", partition_id, source_dir);

        ActiveDataPartSet active_parts(format_version);

        auto detached_parts = getDetachedParts();
        std::erase_if(detached_parts, [&partition_id](const DetachedPartInfo & part_info)
        {
            return !part_info.valid_name || !part_info.prefix.empty() || part_info.partition_id != partition_id;
        });

        for (const auto & part_info : detached_parts)
        {
            LOG_DEBUG(log, "Found part {}", part_info.dir_name);
            active_parts.add(part_info.dir_name);
        }

        LOG_DEBUG(log, "{} of them are active", active_parts.size());

        /// Inactive parts are renamed so they can not be attached in case of repeated ATTACH.
        for (const auto & part_info : detached_parts)
        {
            const String containing_part = active_parts.getContainingPart(part_info.dir_name);

            LOG_DEBUG(log, "Found containing part {} for part {}", containing_part, part_info.dir_name);

            if (!containing_part.empty() && containing_part != part_info.dir_name)
                part_info.disk->moveDirectory(fs::path(relative_data_path) / source_dir / part_info.dir_name,
                    fs::path(relative_data_path) / source_dir / ("inactive_" + part_info.dir_name));
            else
                renamed_parts.addPart(part_info.dir_name, "attaching_" + part_info.dir_name, part_info.disk);
        }
    }


    /// Try to rename all parts before attaching to prevent race with DROP DETACHED and another ATTACH.
    renamed_parts.tryRenameAll();

    /// Synchronously check that added parts exist and are not broken. We will write checksums.txt if it does not exist.
    LOG_DEBUG(log, "Checking {} parts", renamed_parts.old_and_new_names.size());
    MutableDataPartsVector loaded_parts;
    loaded_parts.reserve(renamed_parts.old_and_new_names.size());

    for (const auto & [old_name, new_name, disk] : renamed_parts.old_and_new_names)
    {
        LOG_DEBUG(log, "Checking part {}", new_name);

        auto single_disk_volume = std::make_shared<SingleDiskVolume>("volume_" + old_name, disk);
        auto part = getDataPartBuilder(old_name, single_disk_volume, source_dir + new_name)
            .withPartFormatFromDisk()
            .build();

        loadPartAndFixMetadataImpl(part, local_context, getInMemoryMetadataPtr()->getMetadataVersion(), getSettings()->fsync_after_insert);
        loaded_parts.push_back(part);
    }

    return loaded_parts;
}

namespace
{

inline ReservationPtr checkAndReturnReservation(UInt64 expected_size, ReservationPtr reservation)
{
    if (reservation)
        return reservation;

    throw Exception(ErrorCodes::NOT_ENOUGH_SPACE, "Cannot reserve {}, not enough space", ReadableSize(expected_size));
}

}

ReservationPtr MergeTreeData::reserveSpace(UInt64 expected_size) const
{
    expected_size = std::max(RESERVATION_MIN_ESTIMATION_SIZE, expected_size);
    return getStoragePolicy()->reserveAndCheck(expected_size);
}

ReservationPtr MergeTreeData::reserveSpace(UInt64 expected_size, SpacePtr space)
{
    expected_size = std::max(RESERVATION_MIN_ESTIMATION_SIZE, expected_size);
    auto reservation = tryReserveSpace(expected_size, space);
    return checkAndReturnReservation(expected_size, std::move(reservation));
}

ReservationPtr MergeTreeData::reserveSpace(UInt64 expected_size, const IDataPartStorage & data_part_storage)
{
    expected_size = std::max(RESERVATION_MIN_ESTIMATION_SIZE, expected_size);
    return data_part_storage.reserve(expected_size);
}

ReservationPtr MergeTreeData::tryReserveSpace(UInt64 expected_size, const IDataPartStorage & data_part_storage)
{
    expected_size = std::max(RESERVATION_MIN_ESTIMATION_SIZE, expected_size);
    return data_part_storage.tryReserve(expected_size);
}

ReservationPtr MergeTreeData::tryReserveSpace(UInt64 expected_size, SpacePtr space)
{
    expected_size = std::max(RESERVATION_MIN_ESTIMATION_SIZE, expected_size);
    return space->reserve(expected_size);
}

ReservationPtr MergeTreeData::reserveSpacePreferringTTLRules(
    const StorageMetadataPtr & metadata_snapshot,
    UInt64 expected_size,
    const IMergeTreeDataPart::TTLInfos & ttl_infos,
    time_t time_of_move,
    size_t min_volume_index,
    bool is_insert,
    DiskPtr selected_disk) const
{
    expected_size = std::max(RESERVATION_MIN_ESTIMATION_SIZE, expected_size);

    ReservationPtr reservation = tryReserveSpacePreferringTTLRules(
        metadata_snapshot, expected_size, ttl_infos, time_of_move, min_volume_index, is_insert, selected_disk);

    return checkAndReturnReservation(expected_size, std::move(reservation));
}

ReservationPtr MergeTreeData::tryReserveSpacePreferringTTLRules(
    const StorageMetadataPtr & metadata_snapshot,
    UInt64 expected_size,
    const IMergeTreeDataPart::TTLInfos & ttl_infos,
    time_t time_of_move,
    size_t min_volume_index,
    bool is_insert,
    DiskPtr selected_disk) const
{
    expected_size = std::max(RESERVATION_MIN_ESTIMATION_SIZE, expected_size);
    ReservationPtr reservation;

    auto move_ttl_entry = selectTTLDescriptionForTTLInfos(metadata_snapshot->getMoveTTLs(), ttl_infos.moves_ttl, time_of_move, true);

    if (move_ttl_entry)
    {
        LOG_TRACE(log, "Trying to reserve {} to apply a TTL rule. Will try to reserve in the destination", ReadableSize(expected_size));
        SpacePtr destination_ptr = getDestinationForMoveTTL(*move_ttl_entry);
        bool perform_ttl_move_on_insert = is_insert && destination_ptr && shouldPerformTTLMoveOnInsert(destination_ptr);

        if (!destination_ptr)
        {
            if (move_ttl_entry->destination_type == DataDestinationType::VOLUME && !move_ttl_entry->if_exists)
                LOG_WARNING(
                    log,
                    "Would like to reserve space on volume '{}' by TTL rule of table '{}' but volume was not found",
                    move_ttl_entry->destination_name,
                    log.loadName());
            else if (move_ttl_entry->destination_type == DataDestinationType::DISK && !move_ttl_entry->if_exists)
                LOG_WARNING(
                    log,
                    "Would like to reserve space on disk '{}' by TTL rule of table '{}' but disk was not found",
                    move_ttl_entry->destination_name,
                    log.loadName());
        }
        else if (is_insert && !perform_ttl_move_on_insert)
        {
            LOG_TRACE(
                log,
                "TTL move on insert to {} {} for table {} is disabled",
                (move_ttl_entry->destination_type == DataDestinationType::VOLUME ? "volume" : "disk"),
                move_ttl_entry->destination_name,
                log.loadName());
        }
        else
        {
            reservation = destination_ptr->reserve(expected_size);
            if (reservation)
                return reservation;
            else
                if (move_ttl_entry->destination_type == DataDestinationType::VOLUME)
                    LOG_WARNING(
                        log,
                        "Would like to reserve space on volume '{}' by TTL rule of table '{}' but there is not enough space",
                        move_ttl_entry->destination_name,
                        log.loadName());
                else if (move_ttl_entry->destination_type == DataDestinationType::DISK)
                    LOG_WARNING(
                        log,
                        "Would like to reserve space on disk '{}' by TTL rule of table '{}' but there is not enough space",
                        move_ttl_entry->destination_name,
                        log.loadName());
        }
    }

    // Prefer selected_disk
    if (selected_disk)
        reservation = selected_disk->reserve(expected_size);

    if (!reservation)
        reservation = getStoragePolicy()->reserve(expected_size, min_volume_index);

    return reservation;
}

SpacePtr MergeTreeData::getDestinationForMoveTTL(const TTLDescription & move_ttl) const
{
    auto policy = getStoragePolicy();
    if (move_ttl.destination_type == DataDestinationType::VOLUME)
        return policy->tryGetVolumeByName(move_ttl.destination_name);
    else if (move_ttl.destination_type == DataDestinationType::DISK)
        return policy->tryGetDiskByName(move_ttl.destination_name);
    else
        return {};
}

bool MergeTreeData::shouldPerformTTLMoveOnInsert(const SpacePtr & move_destination) const
{
    if (move_destination->isVolume())
    {
        auto volume = std::static_pointer_cast<IVolume>(move_destination);
        return volume->perform_ttl_move_on_insert;
    }
    if (move_destination->isDisk())
    {
        auto disk = std::static_pointer_cast<IDisk>(move_destination);
        if (auto volume = getStoragePolicy()->tryGetVolumeByDiskName(disk->getName()))
            return volume->perform_ttl_move_on_insert;
    }
    return false;
}

bool MergeTreeData::isPartInTTLDestination(const TTLDescription & ttl, const IMergeTreeDataPart & part) const
{
    auto policy = getStoragePolicy();
    if (ttl.destination_type == DataDestinationType::VOLUME)
    {
        for (const auto & disk : policy->getVolumeByName(ttl.destination_name)->getDisks())
            if (disk->getName() == part.getDataPartStorage().getDiskName())
                return true;
    }
    else if (ttl.destination_type == DataDestinationType::DISK)
        return policy->getDiskByName(ttl.destination_name)->getName() == part.getDataPartStorage().getDiskName();
    return false;
}

CompressionCodecPtr MergeTreeData::getCompressionCodecForPart(size_t part_size_compressed, const IMergeTreeDataPart::TTLInfos & ttl_infos, time_t current_time) const
{
    auto metadata_snapshot = getInMemoryMetadataPtr();

    const auto & recompression_ttl_entries = metadata_snapshot->getRecompressionTTLs();
    auto best_ttl_entry = selectTTLDescriptionForTTLInfos(recompression_ttl_entries, ttl_infos.recompression_ttl, current_time, true);

    if (best_ttl_entry)
        return CompressionCodecFactory::instance().get(best_ttl_entry->recompression_codec, {});

    return getContext()->chooseCompressionCodec(
        part_size_compressed,
        static_cast<double>(part_size_compressed) / getTotalActiveSizeInBytes());
}


MergeTreeData::DataParts MergeTreeData::getDataParts(const DataPartStates & affordable_states) const
{
    DataParts res;
    {
        auto lock = lockParts();
        for (auto state : affordable_states)
        {
            auto range = getDataPartsStateRange(state);
            res.insert(range.begin(), range.end());
        }
    }
    return res;
}

MergeTreeData::DataParts MergeTreeData::getDataPartsForInternalUsage() const
{
    return getDataParts({DataPartState::Active});
}

MergeTreeData::DataPartsVector MergeTreeData::getDataPartsVectorForInternalUsage() const
{
    return getDataPartsVectorForInternalUsage({DataPartState::Active});
}

MergeTreeData::DataPartPtr MergeTreeData::getAnyPartInPartition(
    const String & partition_id, DataPartsLock & /*data_parts_lock*/) const
{
    auto it = data_parts_by_state_and_info.lower_bound(DataPartStateAndPartitionID{DataPartState::Active, partition_id});

    if (it != data_parts_by_state_and_info.end() && (*it)->getState() == DataPartState::Active && (*it)->info.partition_id == partition_id)
        return *it;

    return nullptr;
}


MergeTreeData::Transaction::Transaction(MergeTreeData & data_, MergeTreeTransaction * txn_)
    : data(data_)
    , txn(txn_)
{
    if (txn)
        data.transactions_enabled.store(true);
}

void MergeTreeData::Transaction::rollbackPartsToTemporaryState()
{
    if (!isEmpty())
    {
        WriteBufferFromOwnString buf;
        buf << " Rollbacking parts state to temporary and removing from working set:";
        for (const auto & part : precommitted_parts)
            buf << " " << part->getDataPartStorage().getPartDirectory();
        buf << ".";
        LOG_DEBUG(data.log, "Undoing transaction.{}", buf.str());

        data.removePartsFromWorkingSetImmediatelyAndSetTemporaryState(
            DataPartsVector(precommitted_parts.begin(), precommitted_parts.end()));
    }

    clear();
}

TransactionID MergeTreeData::Transaction::getTID() const
{
    if (txn)
        return txn->tid;
    return Tx::PrehistoricTID;
}

void MergeTreeData::Transaction::addPart(MutableDataPartPtr & part)
{
    precommitted_parts.insert(part);
}

void MergeTreeData::Transaction::rollback()
{
    if (!isEmpty())
    {
        WriteBufferFromOwnString buf;
        buf << "Removing parts:";
        for (const auto & part : precommitted_parts)
            buf << " " << part->getDataPartStorage().getPartDirectory();
        buf << ".";
        LOG_DEBUG(data.log, "Undoing transaction {}. {}", getTID(), buf.str());

        for (const auto & part : precommitted_parts)
            part->version.creation_csn.store(Tx::RolledBackCSN);

        auto lock = data.lockParts();

        if (data.data_parts_indexes.empty())
        {
            /// Table was dropped concurrently and all parts (including PreActive parts) were cleared, so there's nothing to rollback
            if (!data.all_data_dropped)
            {
                Strings part_names;
                for (const auto & part : precommitted_parts)
                    part_names.emplace_back(part->name);
                throw Exception(ErrorCodes::LOGICAL_ERROR, "There are some PreActive parts ({}) to rollback, "
                                "but data parts set is empty and table {} was not dropped. It's a bug",
                                fmt::join(part_names, ", "), data.getStorageID().getNameForLogs());
            }
        }
        else
        {
            data.removePartsFromWorkingSet(txn,
                DataPartsVector(precommitted_parts.begin(), precommitted_parts.end()),
                /* clear_without_timeout = */ true, &lock);
        }
    }

    clear();
}

void MergeTreeData::Transaction::clear()
{
    precommitted_parts.clear();
}

MergeTreeData::DataPartsVector MergeTreeData::Transaction::commit(MergeTreeData::DataPartsLock * acquired_parts_lock)
{
    DataPartsVector total_covered_parts;

    if (!isEmpty())
    {
        auto settings = data.getSettings();
        auto parts_lock = acquired_parts_lock ? MergeTreeData::DataPartsLock() : data.lockParts();
        auto * owing_parts_lock = acquired_parts_lock ? acquired_parts_lock : &parts_lock;

        for (const auto & part : precommitted_parts)
            if (part->getDataPartStorage().hasActiveTransaction())
                part->getDataPartStorage().commitTransaction();

        if (txn)
            for (const auto & part : precommitted_parts)
            {
                DataPartPtr covering_part;
                DataPartsVector covered_active_parts = data.getActivePartsToReplace(part->info, part->name, covering_part, *owing_parts_lock);

                /// outdated parts should be also collected here
                /// the visible outdated parts should be tried to be removed
                /// more likely the conflict happens at the removing visible outdated parts, what is right actually
                DataPartsVector covered_outdated_parts = data.getCoveredOutdatedParts(part, *owing_parts_lock);

                LOG_TEST(data.log, "Got {} oudated parts covered by {} (TID {} CSN {}): {}",
                         covered_outdated_parts.size(), part->getNameWithState(), txn->tid, txn->getSnapshot(), fmt::join(getPartsNames(covered_outdated_parts), ", "));
                data.filterVisibleDataParts(covered_outdated_parts, txn->getSnapshot(), txn->tid);

                DataPartsVector covered_parts;
                covered_parts.reserve(covered_active_parts.size() + covered_outdated_parts.size());
                std::move(covered_active_parts.begin(), covered_active_parts.end(), std::back_inserter(covered_parts));
                std::move(covered_outdated_parts.begin(), covered_outdated_parts.end(), std::back_inserter(covered_parts));

                MergeTreeTransaction::addNewPartAndRemoveCovered(data.shared_from_this(), part, covered_parts, txn);
            }

        MergeTreeData::WriteAheadLogPtr wal;
        auto get_inited_wal = [&] ()
        {
            if (!wal)
                wal = data.getWriteAheadLog();
            return wal;
        };

        if (settings->in_memory_parts_enable_wal)
            for (const auto & part : precommitted_parts)
                if (auto part_in_memory = asInMemoryPart(part))
                    get_inited_wal()->addPart(part_in_memory);

        NOEXCEPT_SCOPE({
            auto current_time = time(nullptr);

            size_t add_bytes = 0;
            size_t add_rows = 0;
            size_t add_parts = 0;

            size_t reduce_bytes = 0;
            size_t reduce_rows = 0;
            size_t reduce_parts = 0;

            for (const auto & part : precommitted_parts)
            {
                DataPartPtr covering_part;
                DataPartsVector covered_parts = data.getActivePartsToReplace(part->info, part->name, covering_part, *owing_parts_lock);
                if (covering_part)
                {
                    /// It's totally fine for zero-level parts, because of possible race condition between ReplicatedMergeTreeSink and
                    /// background queue execution (new part is added to ZK before this function is called,
                    /// so other replica may produce covering part and replication queue may download covering part).
                    if (part->info.level)
                        LOG_WARNING(data.log, "Tried to commit obsolete part {} covered by {}", part->name, covering_part->getNameWithState());
                    else
                        LOG_INFO(data.log, "Tried to commit obsolete part {} covered by {}", part->name, covering_part->getNameWithState());

                    part->remove_time.store(0, std::memory_order_relaxed); /// The part will be removed without waiting for old_parts_lifetime seconds.
                    data.modifyPartState(part, DataPartState::Outdated);
                }
                else
                {
                    if (!txn)
                        MergeTreeTransaction::addNewPartAndRemoveCovered(data.shared_from_this(), part, covered_parts, NO_TRANSACTION_RAW);

                    total_covered_parts.insert(total_covered_parts.end(), covered_parts.begin(), covered_parts.end());
                    for (const auto & covered_part : covered_parts)
                    {
                        covered_part->remove_time.store(current_time, std::memory_order_relaxed);

                        reduce_bytes += covered_part->getBytesOnDisk();
                        reduce_rows += covered_part->rows_count;

                        data.modifyPartState(covered_part, DataPartState::Outdated);
                        data.removePartContributionToColumnAndSecondaryIndexSizes(covered_part);

                        if (settings->in_memory_parts_enable_wal)
                            if (isInMemoryPart(covered_part))
                                get_inited_wal()->dropPart(covered_part->name);
                    }

                    reduce_parts += covered_parts.size();

                    add_bytes += part->getBytesOnDisk();
                    add_rows += part->rows_count;
                    ++add_parts;

                    data.modifyPartState(part, DataPartState::Active);
                    data.addPartContributionToColumnAndSecondaryIndexSizes(part);
                }
            }

            /// if (reduce_parts == 0)
            /// {
            ///     for (const auto & part : precommitted_parts)
            ///         data.updateObjectColumns(part, parts_lock);
            /// }
            /// else
            ///     data.resetObjectColumnsFromActiveParts(parts_lock);

            ssize_t diff_bytes = add_bytes - reduce_bytes;
            ssize_t diff_rows = add_rows - reduce_rows;
            ssize_t diff_parts  = add_parts - reduce_parts;
            data.increaseDataVolume(diff_bytes, diff_rows, diff_parts);
        });
    }

    clear();

    return total_covered_parts;
}

bool MergeTreeData::isPrimaryOrMinMaxKeyColumnPossiblyWrappedInFunctions(
    const ASTPtr & node, const StorageMetadataPtr & metadata_snapshot) const
{
    const String column_name = node->getColumnName();

    for (const auto & name : metadata_snapshot->getPrimaryKeyColumns())
        if (column_name == name)
            return true;

    for (const auto & name : getMinMaxColumnsNames(metadata_snapshot->getPartitionKey()))
        if (column_name == name)
            return true;

    if (const auto * func = node->as<ASTFunction>())
        if (func->arguments->children.size() == 1)
            return isPrimaryOrMinMaxKeyColumnPossiblyWrappedInFunctions(func->arguments->children.front(), metadata_snapshot);

    return false;
}

bool MergeTreeData::mayBenefitFromIndexForIn(
    const ASTPtr & left_in_operand, ContextPtr, const StorageMetadataPtr & metadata_snapshot) const
{
    /// Make sure that the left side of the IN operator contain part of the key.
    /// If there is a tuple on the left side of the IN operator, at least one item of the tuple
    ///  must be part of the key (probably wrapped by a chain of some acceptable functions).
    const auto * left_in_operand_tuple = left_in_operand->as<ASTFunction>();
    const auto & index_wrapper_factory = MergeTreeIndexFactory::instance();
    if (left_in_operand_tuple && left_in_operand_tuple->name == "tuple_cast")
    {
        for (const auto & item : left_in_operand_tuple->arguments->children)
        {
            if (isPrimaryOrMinMaxKeyColumnPossiblyWrappedInFunctions(item, metadata_snapshot))
                return true;
            for (const auto & index : metadata_snapshot->getSecondaryIndices())
                if (index_wrapper_factory.get(index)->mayBenefitFromIndexForIn(item))
                    return true;
            for (const auto & projection : metadata_snapshot->getProjections())
            {
                if (projection.isPrimaryKeyColumnPossiblyWrappedInFunctions(item))
                    return true;
            }
        }
        /// The tuple itself may be part of the primary key, so check that as a last resort.
        if (isPrimaryOrMinMaxKeyColumnPossiblyWrappedInFunctions(left_in_operand, metadata_snapshot))
            return true;
        for (const auto & projection : metadata_snapshot->getProjections())
        {
            if (projection.isPrimaryKeyColumnPossiblyWrappedInFunctions(left_in_operand))
                return true;
        }
        return false;
    }
    else
    {
        for (const auto & index : metadata_snapshot->getSecondaryIndices())
            if (index_wrapper_factory.get(index)->mayBenefitFromIndexForIn(left_in_operand))
                return true;

        for (const auto & projection : metadata_snapshot->getProjections())
        {
            if (projection.isPrimaryKeyColumnPossiblyWrappedInFunctions(left_in_operand))
                return true;
        }
        return isPrimaryOrMinMaxKeyColumnPossiblyWrappedInFunctions(left_in_operand, metadata_snapshot);
    }
}

using PartitionIdToMaxBlock = std::unordered_map<String, Int64>;

static void selectBestProjection(
    const MergeTreeDataSelectExecutor & reader,
    const StorageSnapshotPtr & storage_snapshot,
    const SelectQueryInfo & query_info,
    const ActionDAGNodes & added_filter_nodes,
    const Names & required_columns,
    ProjectionCandidate & candidate,
    ContextPtr query_context,
    std::shared_ptr<PartitionIdToMaxBlock> max_added_blocks,
    const Settings & settings,
    const MergeTreeData::DataPartsVector & parts,
    ProjectionCandidate *& selected_candidate,
    size_t & min_sum_marks)
{
    MergeTreeData::DataPartsVector projection_parts;
    MergeTreeData::DataPartsVector normal_parts;
    for (const auto & part : parts)
    {
        const auto & projections = part->getProjectionParts();
        auto it = projections.find(candidate.desc->name);
        if (it != projections.end())
            projection_parts.push_back(it->second);
        else
            normal_parts.push_back(part);
    }

    if (projection_parts.empty())
        return;

    auto projection_result_ptr = reader.estimateNumMarksToRead(
        projection_parts,
        candidate.prewhere_info,
        candidate.required_columns,
        storage_snapshot->metadata,
        candidate.desc->metadata,
        query_info,
        added_filter_nodes,
        query_context,
        settings.max_threads,
        max_added_blocks);

    if (projection_result_ptr->error())
        return;

    auto sum_marks = projection_result_ptr->marks();
    if (normal_parts.empty())
    {
        // All parts are projection parts which allows us to use in_order_optimization.
        // TODO It might be better to use a complete projection even with more marks to read.
        candidate.complete = true;
    }
    else
    {
        auto normal_result_ptr = reader.estimateNumMarksToRead(
            normal_parts,
            query_info.prewhere_info,
            required_columns,
            storage_snapshot->metadata,
            storage_snapshot->metadata,
            query_info, // TODO syntax_analysis_result set in index
            added_filter_nodes,
            query_context,
            settings.max_threads,
            max_added_blocks);

        if (normal_result_ptr->error())
            return;

        if (normal_result_ptr->marks() == 0)
            candidate.complete = true;
        else
        {
            sum_marks += normal_result_ptr->marks();
            candidate.merge_tree_normal_select_result_ptr = normal_result_ptr;
        }
    }
    candidate.merge_tree_projection_select_result_ptr = projection_result_ptr;

    // We choose the projection with least sum_marks to read.
    if (sum_marks < min_sum_marks)
    {
        selected_candidate = &candidate;
        min_sum_marks = sum_marks;
    }
}


Block MergeTreeData::getMinMaxCountProjectionBlock(
    const StorageMetadataPtr & metadata_snapshot,
    const Names & required_columns,
    const SelectQueryInfo & query_info,
    const DataPartsVector & parts,
    const PartitionIdToMaxBlock * max_block_numbers_to_read,
    ContextPtr query_context) const
{
    if (!metadata_snapshot->minmax_count_projection)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Cannot find the definition of minmax_count projection but it's used in current query. "
                        "It's a bug");

    auto block = metadata_snapshot->minmax_count_projection->sample_block.cloneEmpty();
    bool need_primary_key_max_column = false;
    const auto & primary_key_max_column_name = metadata_snapshot->minmax_count_projection->primary_key_max_column_name;
    if (!primary_key_max_column_name.empty())
    {
        need_primary_key_max_column = std::any_of(
            required_columns.begin(), required_columns.end(), [&](const auto & name) { return primary_key_max_column_name == name; });
    }

    auto partition_minmax_count_columns = block.mutateColumns();
    auto insert = [](ColumnAggregateFunction & column, const Field & value)
    {
        auto func = column.getAggregateFunction();
        Arena & arena = column.createOrGetArena();
        size_t size_of_state = func->sizeOfData();
        size_t align_of_state = func->alignOfData();
        auto * place = arena.alignedAlloc(size_of_state, align_of_state);
        func->create(place);
        auto value_column = func->getResultType()->createColumnConst(1, value)->convertToFullColumnIfConst();
        const auto * value_column_ptr = value_column.get();
        func->add(place, &value_column_ptr, 0, &arena);
        column.insertFrom(place);
    };

    ASTPtr expression_ast;
    Block virtual_columns_block = getBlockWithVirtualPartColumns(parts, false /* one_part */, true /* ignore_empty */);
    if (virtual_columns_block.rows() == 0)
        return {};

    std::optional<PartitionPruner> partition_pruner;
    std::optional<KeyCondition> minmax_idx_condition;
    DataTypes minmax_columns_types;
    if (metadata_snapshot->hasPartitionKey())
    {
        const auto & partition_key = metadata_snapshot->getPartitionKey();
        auto minmax_columns_names = getMinMaxColumnsNames(partition_key);
        minmax_columns_types = getMinMaxColumnsTypes(partition_key);

        minmax_idx_condition.emplace(
            query_info, query_context, minmax_columns_names,
            getMinMaxExpr(partition_key, ExpressionActionsSettings::fromContext(query_context)));
        partition_pruner.emplace(metadata_snapshot, query_info, query_context, false /* strict */);
    }

    // Generate valid expressions for filtering
    VirtualColumnUtils::prepareFilterBlockWithQuery(query_info.query, query_context, virtual_columns_block, expression_ast);
    if (expression_ast)
        VirtualColumnUtils::filterBlockWithQuery(query_info.query, virtual_columns_block, query_context, expression_ast);

    size_t rows = virtual_columns_block.rows();
    const ColumnString & part_name_column = typeid_cast<const ColumnString &>(*virtual_columns_block.getByName("_part").column);
    size_t part_idx = 0;
    auto filter_column = ColumnUInt8::create();
    auto & filter_column_data = filter_column->getData();
    for (size_t row = 0; row < rows; ++row)
    {
        while (parts[part_idx]->name != part_name_column.getDataAt(row))
            ++part_idx;

        const auto & part = parts[part_idx];

        if (!part->minmax_idx->initialized)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Found a non-empty part with uninitialized minmax_idx. It's a bug");

        filter_column_data.emplace_back();

        if (max_block_numbers_to_read)
        {
            auto blocks_iterator = max_block_numbers_to_read->find(part->info.partition_id);
            if (blocks_iterator == max_block_numbers_to_read->end() || part->info.max_block > blocks_iterator->second)
                continue;
        }

        if (minmax_idx_condition
            && !minmax_idx_condition->checkInHyperrectangle(part->minmax_idx->hyperrectangle, minmax_columns_types).can_be_true)
            continue;

        if (partition_pruner)
        {
            if (partition_pruner->canBePruned(*part))
                continue;
        }

        /// It's extremely rare that some parts have final marks while others don't. To make it
        /// straightforward, disable minmax_count projection when `max(pk)' encounters any part with
        /// no final mark.
        if (need_primary_key_max_column && !part->index_granularity.hasFinalMark())
            return {};

        filter_column_data.back() = 1;
        size_t pos = 0;
        for (size_t i : metadata_snapshot->minmax_count_projection->partition_value_indices)
        {
            if (i >= part->partition.value.size())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Partition value index is out of boundary. It's a bug");
            partition_minmax_count_columns[pos++]->insert(part->partition.value[i]);
        }

        size_t minmax_idx_size = part->minmax_idx->hyperrectangle.size();
        for (size_t i = 0; i < minmax_idx_size; ++i)
        {
            auto & min_column = assert_cast<ColumnAggregateFunction &>(*partition_minmax_count_columns[pos++]);
            auto & max_column = assert_cast<ColumnAggregateFunction &>(*partition_minmax_count_columns[pos++]);
            const auto & range = part->minmax_idx->hyperrectangle[i];
            insert(min_column, range.left);
            insert(max_column, range.right);
        }

        if (!primary_key_max_column_name.empty())
        {
            const auto & primary_key_column = *part->index[0];
            auto & min_column = assert_cast<ColumnAggregateFunction &>(*partition_minmax_count_columns[pos++]);
            auto & max_column = assert_cast<ColumnAggregateFunction &>(*partition_minmax_count_columns[pos++]);
            insert(min_column, primary_key_column[0]);
            insert(max_column, primary_key_column[primary_key_column.size() - 1]);
        }

        {
            auto & column = assert_cast<ColumnAggregateFunction &>(*partition_minmax_count_columns.back());
            auto func = column.getAggregateFunction();
            Arena & arena = column.createOrGetArena();
            size_t size_of_state = func->sizeOfData();
            size_t align_of_state = func->alignOfData();
            auto * place = arena.alignedAlloc(size_of_state, align_of_state);
            func->create(place);
            const AggregateFunctionCount & agg_count = assert_cast<const AggregateFunctionCount &>(*func);
            agg_count.set(place, part->rows_count);
            column.insertFrom(place);
        }
    }
    block.setColumns(std::move(partition_minmax_count_columns));

    FilterDescription filter(*filter_column);
    for (size_t i = 0; i < virtual_columns_block.columns(); ++i)
    {
        ColumnPtr & column = virtual_columns_block.safeGetByPosition(i).column;
        column = column->filter(*filter.data, -1);
    }

    if (block.rows() == 0)
        return {};

    Block res;
    for (const auto & name : required_columns)
    {
        if (virtual_columns_block.has(name))
            res.insert(virtual_columns_block.getByName(name));
        else if (block.has(name))
            res.insert(block.getByName(name));
        else
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cannot find column {} in minmax_count projection but query analysis still selects this projection. It's a bug",
                name);
    }
    return res;
}


std::optional<ProjectionCandidate> MergeTreeData::getQueryProcessingStageWithAggregateProjection(
    ContextPtr query_context, const StorageSnapshotPtr & storage_snapshot, SelectQueryInfo & query_info) const
{
    const auto & metadata_snapshot = storage_snapshot->metadata;
    const auto & settings = query_context->getSettingsRef();
    if (!settings.allow_experimental_projection_optimization || query_info.ignore_projections || query_info.is_projection_query)
        return std::nullopt;

    // Currently projections don't support parallel replicas reading yet.
    if (settings.parallel_replicas_count > 1 || settings.max_parallel_replicas > 1)
        return std::nullopt;

    auto query_ptr = query_info.original_query;
    auto * select_query = query_ptr->as<ASTSelectQuery>();
    if (!select_query)
        return std::nullopt;

    // Currently projections don't support final yet.
    if (select_query->final())
        return std::nullopt;

    // Currently projections don't support sample yet.
    if (select_query->sampleSize())
        return std::nullopt;

    // Currently projections don't support ARRAY JOIN yet.
    if (select_query->arrayJoinExpressionList().first)
        return std::nullopt;

    // In order to properly analyze joins, aliases should be recognized. However, aliases get lost during projection analysis.
    // Let's disable projection if there are any JOIN clauses.
    // TODO: We need a better identifier resolution mechanism for projection analysis.
    if (select_query->hasJoin())
        return std::nullopt;

    // INTERPOLATE expressions may include aliases, so aliases should be preserved
    if (select_query->interpolate() && !select_query->interpolate()->children.empty())
        return std::nullopt;

    // Projections don't support grouping sets yet.
    if (select_query->group_by_with_grouping_sets
        || select_query->group_by_with_totals
        || select_query->group_by_with_rollup
        || select_query->group_by_with_cube)
        return std::nullopt;

    auto query_options = SelectQueryOptions(
        QueryProcessingStage::WithMergeableState,
        /* depth */ 1,
        /* is_subquery_= */ true
        ).ignoreProjections().ignoreAlias();

    InterpreterSelectQuery select(
        query_ptr,
        query_context,
        query_options,
        query_info.prepared_sets);

    const auto & analysis_result = select.getAnalysisResult();

    query_info.prepared_sets = select.getQueryAnalyzer()->getPreparedSets();

    const auto & before_where = analysis_result.before_where;
    const auto & where_column_name = analysis_result.where_column_name;


    /// For PK analysis
    ActionDAGNodes added_filter_nodes;
    if (auto additional_filter_info = select.getAdditionalQueryInfo())
        added_filter_nodes.nodes.push_back(&additional_filter_info->actions->findInOutputs(additional_filter_info->column_name));

    if (before_where)
        added_filter_nodes.nodes.push_back(&before_where->findInOutputs(where_column_name));

    bool can_use_aggregate_projection = true;
    /// If the first stage of the query pipeline is more complex than Aggregating - Expression - Filter - ReadFromStorage,
    /// we cannot use aggregate projection.
    if (analysis_result.join != nullptr || analysis_result.array_join != nullptr)
        can_use_aggregate_projection = false;

    /// Check if all needed columns can be provided by some aggregate projection. Here we also try
    /// to find expression matches. For example, suppose an aggregate projection contains a column
    /// named sum(x) and the given query also has an expression called sum(x), it's a match. This is
    /// why we need to ignore all aliases during projection creation and the above query planning.
    /// It's also worth noting that, sqrt(sum(x)) will also work because we can treat sum(x) as a
    /// required column.

    /// The ownership of ProjectionDescription is hold in metadata_snapshot which lives along with
    /// InterpreterSelect, thus we can store the raw pointer here.
    std::vector<ProjectionCandidate> candidates;
    NameSet keys;
    std::unordered_map<std::string_view, size_t> key_name_pos_map;
    size_t pos = 0;
    for (const auto & desc : select.getQueryAnalyzer()->aggregationKeys())
    {
        keys.insert(desc.name);
        key_name_pos_map.insert({desc.name, pos++});
    }
    auto actions_settings = ExpressionActionsSettings::fromSettings(settings, CompileExpressions::yes);

    // All required columns should be provided by either current projection or previous actions
    // Let's traverse backward to finish the check.
    // TODO what if there is a column with name sum(x) and an aggregate sum(x)?
    auto rewrite_before_where =
        [&](ProjectionCandidate & candidate, const ProjectionDescription & projection,
            NameSet & required_columns, const Block & source_block, const Block & aggregates)
    {
        if (analysis_result.before_where)
        {
            candidate.where_column_name = analysis_result.where_column_name;
            candidate.remove_where_filter = analysis_result.remove_where_filter;
            candidate.before_where = analysis_result.before_where->clone();

            auto new_required_columns = candidate.before_where->foldActionsByProjection(
                required_columns,
                projection.sample_block_for_keys,
                candidate.where_column_name);
            if (new_required_columns.empty() && !required_columns.empty())
                return false;
            required_columns = std::move(new_required_columns);
            candidate.before_where->addAggregatesViaProjection(aggregates);
        }

        if (analysis_result.prewhere_info)
        {
            candidate.prewhere_info = analysis_result.prewhere_info;

            auto prewhere_actions = candidate.prewhere_info->prewhere_actions->clone();
            auto prewhere_required_columns = required_columns;
            // required_columns should not contain columns generated by prewhere
            for (const auto & column : prewhere_actions->getResultColumns())
                required_columns.erase(column.name);

            {
                // prewhere_action should not add missing keys.
                auto new_prewhere_required_columns = prewhere_actions->foldActionsByProjection(
                        prewhere_required_columns, projection.sample_block_for_keys, candidate.prewhere_info->prewhere_column_name, false);
                if (new_prewhere_required_columns.empty() && !prewhere_required_columns.empty())
                    return false;
                prewhere_required_columns = std::move(new_prewhere_required_columns);
                candidate.prewhere_info->prewhere_actions = prewhere_actions;
            }

            if (candidate.prewhere_info->row_level_filter)
            {
                auto row_level_filter_actions = candidate.prewhere_info->row_level_filter->clone();
                // row_level_filter_action should not add missing keys.
                auto new_prewhere_required_columns = row_level_filter_actions->foldActionsByProjection(
                    prewhere_required_columns, projection.sample_block_for_keys, candidate.prewhere_info->row_level_column_name, false);
                if (new_prewhere_required_columns.empty() && !prewhere_required_columns.empty())
                    return false;
                prewhere_required_columns = std::move(new_prewhere_required_columns);
                candidate.prewhere_info->row_level_filter = row_level_filter_actions;
            }

            required_columns.insert(prewhere_required_columns.begin(), prewhere_required_columns.end());
        }

        bool match = true;
        for (const auto & column : required_columns)
        {
            /// There are still missing columns, fail to match
            if (!source_block.has(column))
            {
                match = false;
                break;
            }
        }
        return match;
    };

    auto virtual_block = getSampleBlockWithVirtualColumns();
    auto add_projection_candidate = [&](const ProjectionDescription & projection)
    {
        ProjectionCandidate candidate{};
        candidate.desc = &projection;

        auto sample_block = projection.sample_block;
        auto sample_block_for_keys = projection.sample_block_for_keys;
        for (const auto & column : virtual_block)
        {
            sample_block.insertUnique(column);
            sample_block_for_keys.insertUnique(column);
        }

        // If optimize_aggregation_in_order = true, we need additional information to transform the projection's pipeline.
        auto attach_aggregation_in_order_info = [&]()
        {
            for (const auto & desc : select.getQueryAnalyzer()->aggregationKeys())
            {
                const String & key = desc.name;
                auto actions_dag = analysis_result.before_aggregation->clone();
                actions_dag->foldActionsByProjection({key}, sample_block_for_keys);
                candidate.group_by_elements_actions.emplace_back(std::make_shared<ExpressionActions>(actions_dag, actions_settings));
                candidate.group_by_elements_order_descr.emplace_back(key, 1, 1);
            }
        };

        if (projection.type == ProjectionDescription::Type::Aggregate && analysis_result.need_aggregate && can_use_aggregate_projection)
        {
            bool match = true;
            Block aggregates;
            // Let's first check if all aggregates are provided by current projection
            for (const auto & aggregate : select.getQueryAnalyzer()->aggregates())
            {
                const auto * column = sample_block.findByName(aggregate.column_name);
                if (column)
                    aggregates.insert(*column);
                else
                {
                    match = false;
                    break;
                }
            }
            if (!match)
                return;

            // Check if all aggregation keys can be either provided by some action, or by current
            // projection directly. Reshape the `before_aggregation` action DAG so that it only
            // needs to provide aggregation keys, and the DAG of certain child might be substituted
            // by some keys in projection.
            candidate.before_aggregation = analysis_result.before_aggregation->clone();
            auto required_columns = candidate.before_aggregation->foldActionsByProjection(keys, sample_block_for_keys);

            // TODO Let's find out the exact required_columns for keys.
            if (required_columns.empty() && (!keys.empty() && !candidate.before_aggregation->getRequiredColumns().empty()))
                return;

            if (analysis_result.optimize_aggregation_in_order)
                attach_aggregation_in_order_info();

            // Reorder aggregation keys and attach aggregates
            candidate.before_aggregation->reorderAggregationKeysForProjection(key_name_pos_map);
            candidate.before_aggregation->addAggregatesViaProjection(aggregates);

            if (rewrite_before_where(candidate, projection, required_columns, sample_block_for_keys, aggregates))
            {
                candidate.required_columns = {required_columns.begin(), required_columns.end()};
                for (const auto & aggregate : aggregates)
                    candidate.required_columns.push_back(aggregate.name);
                candidates.push_back(std::move(candidate));
            }
        }
        else if (projection.type == ProjectionDescription::Type::Normal)
        {
            if (analysis_result.before_aggregation && analysis_result.optimize_aggregation_in_order)
                attach_aggregation_in_order_info();

            if (analysis_result.hasWhere() || analysis_result.hasPrewhere())
            {
                const auto & actions
                    = analysis_result.before_aggregation ? analysis_result.before_aggregation : analysis_result.before_order_by;
                NameSet required_columns;
                for (const auto & column : actions->getRequiredColumns())
                    required_columns.insert(column.name);

                if (rewrite_before_where(candidate, projection, required_columns, sample_block, {}))
                {
                    candidate.required_columns = {required_columns.begin(), required_columns.end()};
                    candidates.push_back(std::move(candidate));
                }
            }
        }
    };

    ProjectionCandidate * selected_candidate = nullptr;
    size_t min_sum_marks = std::numeric_limits<size_t>::max();
    if (metadata_snapshot->minmax_count_projection && !has_lightweight_delete_parts.load(std::memory_order_relaxed)) /// Disable ReadFromStorage for parts with lightweight.
        add_projection_candidate(*metadata_snapshot->minmax_count_projection);
    std::optional<ProjectionCandidate> minmax_count_projection_candidate;
    if (!candidates.empty())
    {
        minmax_count_projection_candidate.emplace(std::move(candidates.front()));
        candidates.clear();
    }
    MergeTreeDataSelectExecutor reader(*this);
    std::shared_ptr<PartitionIdToMaxBlock> max_added_blocks;

    const auto & snapshot_data = assert_cast<const MergeTreeData::SnapshotData &>(*storage_snapshot->data);
    const auto & parts = snapshot_data.parts;

    auto prepare_min_max_count_projection = [&]()
    {
        DataPartsVector normal_parts;
        query_info.minmax_count_projection_block = getMinMaxCountProjectionBlock(
            metadata_snapshot,
            minmax_count_projection_candidate->required_columns,
            query_info,
            parts,
            max_added_blocks.get(),
            query_context);

        // minmax_count_projection should not be used when there is no data to process.
        if (!query_info.minmax_count_projection_block)
            return;

        if (minmax_count_projection_candidate->prewhere_info)
        {
            const auto & prewhere_info = minmax_count_projection_candidate->prewhere_info;

            if (prewhere_info->row_level_filter)
            {
                ExpressionActions(prewhere_info->row_level_filter, actions_settings).execute(query_info.minmax_count_projection_block);
                query_info.minmax_count_projection_block.erase(prewhere_info->row_level_column_name);
            }

            if (prewhere_info->prewhere_actions)
                ExpressionActions(prewhere_info->prewhere_actions, actions_settings).execute(query_info.minmax_count_projection_block);

            if (prewhere_info->remove_prewhere_column)
                query_info.minmax_count_projection_block.erase(prewhere_info->prewhere_column_name);
        }

        if (normal_parts.empty())
        {
            selected_candidate = &*minmax_count_projection_candidate;
            selected_candidate->complete = true;
            min_sum_marks = query_info.minmax_count_projection_block.rows();
        }
        else if (normal_parts.size() < parts.size())
        {
            auto normal_result_ptr = reader.estimateNumMarksToRead(
                normal_parts,
                query_info.prewhere_info,
                analysis_result.required_columns,
                metadata_snapshot,
                metadata_snapshot,
                query_info,
                added_filter_nodes,
                query_context,
                settings.max_threads,
                max_added_blocks);

            if (!normal_result_ptr->error())
            {
                selected_candidate = &*minmax_count_projection_candidate;
                selected_candidate->merge_tree_normal_select_result_ptr = normal_result_ptr;
                min_sum_marks = query_info.minmax_count_projection_block.rows() + normal_result_ptr->marks();
            }
        }
    };

    // If minmax_count_projection is a valid candidate, prepare it and check its completeness.
    if (minmax_count_projection_candidate)
        prepare_min_max_count_projection();

    // We cannot find a complete match of minmax_count_projection, add more projections to check.
    if (!selected_candidate || !selected_candidate->complete)
        for (const auto & projection : metadata_snapshot->projections)
            add_projection_candidate(projection);

    // Let's select the best projection to execute the query.
    if (!candidates.empty())
    {
        query_info.merge_tree_select_result_ptr = reader.estimateNumMarksToRead(
            parts,
            query_info.prewhere_info,
            analysis_result.required_columns,
            metadata_snapshot,
            metadata_snapshot,
            query_info,
            added_filter_nodes,
            query_context,
            settings.max_threads,
            max_added_blocks);

        if (!query_info.merge_tree_select_result_ptr->error())
        {
            // Add 1 to base sum_marks so that we prefer projections even when they have equal number of marks to read.
            // NOTE: It is not clear if we need it. E.g. projections do not support skip index for now.
            auto sum_marks = query_info.merge_tree_select_result_ptr->marks() + 1;
            if (sum_marks < min_sum_marks)
            {
                selected_candidate = nullptr;
                min_sum_marks = sum_marks;
            }
        }

        /// Favor aggregate projections
        for (auto & candidate : candidates)
        {
            if (candidate.desc->type == ProjectionDescription::Type::Aggregate)
            {
                selectBestProjection(
                    reader,
                    storage_snapshot,
                    query_info,
                    added_filter_nodes,
                    analysis_result.required_columns,
                    candidate,
                    query_context,
                    max_added_blocks,
                    settings,
                    parts,
                    selected_candidate,
                    min_sum_marks);
            }
        }

        /// Select the best normal projection.
        for (auto & candidate : candidates)
        {
            if (candidate.desc->type == ProjectionDescription::Type::Normal)
            {
                selectBestProjection(
                    reader,
                    storage_snapshot,
                    query_info,
                    added_filter_nodes,
                    analysis_result.required_columns,
                    candidate,
                    query_context,
                    max_added_blocks,
                    settings,
                    parts,
                    selected_candidate,
                    min_sum_marks);
            }
        }
    }

    if (!selected_candidate)
        return std::nullopt;
    else if (min_sum_marks == 0)
    {
        /// If selected_projection indicated an empty result set. Remember it in query_info but
        /// don't use projection to run the query, because projection pipeline with empty result
        /// set will not work correctly with empty_result_for_aggregation_by_empty_set.
        query_info.merge_tree_empty_result = true;
        return std::nullopt;
    }

    if (selected_candidate->desc->type == ProjectionDescription::Type::Aggregate)
    {
        selected_candidate->aggregation_keys = select.getQueryAnalyzer()->aggregationKeys();
        selected_candidate->aggregate_descriptions = select.getQueryAnalyzer()->aggregates();
    }

    return *selected_candidate;
}


QueryProcessingStage::Enum MergeTreeData::getQueryProcessingStage(
    ContextPtr query_context,
    QueryProcessingStage::Enum to_stage,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info) const
{
    if (query_context->getClientInfo().collaborate_with_initiator)
        return QueryProcessingStage::Enum::FetchColumns;

    if (query_context->getSettingsRef().allow_experimental_parallel_reading_from_replicas
        && !query_context->getClientInfo().collaborate_with_initiator
        && to_stage >= QueryProcessingStage::WithMergeableState)
        return QueryProcessingStage::Enum::WithMergeableState;

    if (to_stage >= QueryProcessingStage::Enum::WithMergeableState)
    {
        if (auto projection = getQueryProcessingStageWithAggregateProjection(query_context, storage_snapshot, query_info))
        {
            query_info.projection = std::move(projection);
            if (query_info.projection->desc->type == ProjectionDescription::Type::Aggregate)
                return QueryProcessingStage::Enum::WithMergeableState;
        }
        else
            query_info.projection = std::nullopt;
    }

    return QueryProcessingStage::Enum::FetchColumns;
}

void MergeTreeData::checkColumnFilenamesForCollision(const StorageInMemoryMetadata & metadata, bool throw_on_error) const
{
    auto settings = getDefaultSettings();
    if (metadata.settings_changes)
    {
        const auto & changes = metadata.settings_changes->as<const ASTSetQuery &>().changes;
        settings->applyChanges(changes);
    }

    checkColumnFilenamesForCollision(metadata.getColumns(), *settings, throw_on_error);
}

void MergeTreeData::checkColumnFilenamesForCollision(const ColumnsDescription & columns, const MergeTreeSettings & settings, bool throw_on_error) const
{
    std::unordered_map<String, std::pair<String, String>> stream_name_to_full_name;
    auto columns_list = Nested::collect(columns.getAllPhysical());

    for (const auto & column : columns_list)
    {
        std::unordered_map<String, String> column_streams;

        auto callback = [&](const auto & substream_path)
        {
            String stream_name;
            auto full_stream_name = ISerialization::getFileNameForStream(column, substream_path);

            if (settings.replace_long_file_name_to_hash && full_stream_name.size() > settings.max_file_name_length)
                stream_name = sipHash128String(full_stream_name);
            else
                stream_name = full_stream_name;

            column_streams.emplace(stream_name, full_stream_name);
        };

        auto serialization = column.type->getDefaultSerialization();
        serialization->enumerateStreams(callback);

        if (column.type->supportsSparseSerialization() && settings.ratio_of_defaults_for_sparse_serialization < 1.0)
        {
            auto sparse_serialization = column.type->getSparseSerialization();
            sparse_serialization->enumerateStreams(callback);
        }

        for (const auto & [stream_name, full_stream_name] : column_streams)
        {
            auto [it, inserted] = stream_name_to_full_name.emplace(stream_name, std::pair{full_stream_name, column.name});
            if (!inserted)
            {
                const auto & [other_full_name, other_column_name] = it->second;
                auto other_type = columns.getPhysical(other_column_name).type;

                auto message = fmt::format(
                    "Columns '{} {}' and '{} {}' have streams ({} and {}) with collision in file name {}",
                    column.name, column.type->getName(), other_column_name, other_type->getName(), full_stream_name, other_full_name, stream_name);

                if (settings.replace_long_file_name_to_hash)
                    message += ". It may be a collision between a filename for one column and a hash of filename for another column (see setting 'replace_long_file_name_to_hash')";

                if (throw_on_error)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "{}", message);

                LOG_ERROR(log, "Table definition is incorrect. {}. It may lead to corruption of data or crashes. You need to resolve it manually", message);
                return;
            }
        }
    }
}

MergeTreeData & MergeTreeData::checkStructureAndGetMergeTreeData(IStorage & source_table, const StorageMetadataPtr & src_snapshot, const StorageMetadataPtr & my_snapshot) const
{
    MergeTreeData * src_data = dynamic_cast<MergeTreeData *>(&source_table);
    if (!src_data)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                        "Stream {} supports attachPartitionFrom only for the current engine family. Got {}",
                        source_table.getStorageID().getNameForLogs(), source_table.getName());

    if (my_snapshot->getColumns().getAllPhysical().sizeOfDifference(src_snapshot->getColumns().getAllPhysical()))
        throw Exception(ErrorCodes::INCOMPATIBLE_COLUMNS, "Tables have different structure");

    auto query_to_string = [] (const ASTPtr & ast)
    {
        return ast ? queryToString(ast) : "";
    };

    if (query_to_string(my_snapshot->getSortingKeyAST()) != query_to_string(src_snapshot->getSortingKeyAST()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Tables have different ordering");

    if (query_to_string(my_snapshot->getPartitionKeyAST()) != query_to_string(src_snapshot->getPartitionKeyAST()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Tables have different partition key");

    if (format_version != src_data->format_version)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Tables have different format_version");

    return *src_data;
}

MergeTreeData & MergeTreeData::checkStructureAndGetMergeTreeData(
    const StoragePtr & source_table, const StorageMetadataPtr & src_snapshot, const StorageMetadataPtr & my_snapshot) const
{
    return checkStructureAndGetMergeTreeData(*source_table, src_snapshot, my_snapshot);
}

std::pair<MergeTreeData::MutableDataPartPtr, scope_guard> MergeTreeData::cloneAndLoadDataPartOnSameDisk(
    const MergeTreeData::DataPartPtr & src_part,
    const String & tmp_part_prefix,
    const MergeTreePartInfo & dst_part_info,
    const StorageMetadataPtr & metadata_snapshot,
    const IDataPartStorage::ClonePartParams & params,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings)
{
    chassert(!isStaticStorage());

    /// Check that the storage policy contains the disk where the src_part is located.
    bool does_storage_policy_allow_same_disk = false;
    for (const DiskPtr & disk : getStoragePolicy()->getDisks())
    {
        if (disk->getName() == src_part->getDataPartStorage().getDiskName())
        {
            does_storage_policy_allow_same_disk = true;
            break;
        }
    }
    if (!does_storage_policy_allow_same_disk)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Could not clone and load part {} because disk does not belong to storage policy",
            quoteString(src_part->getDataPartStorage().getFullPath()));

    String dst_part_name = src_part->getNewName(dst_part_info);
    String tmp_dst_part_name = tmp_part_prefix + dst_part_name;
    auto temporary_directory_lock = getTemporaryPartDirectoryHolder(tmp_dst_part_name);

    /// Why it is needed if we only hardlink files?
    auto reservation = src_part->getDataPartStorage().reserve(src_part->getBytesOnDisk());
    auto src_part_storage = src_part->getDataPartStoragePtr();

    scope_guard src_flushed_tmp_dir_lock;
    MergeTreeData::MutableDataPartPtr src_flushed_tmp_part;

    /// If source part is in memory, flush it to disk and clone it already in on-disk format
    /// Protect tmp dir from removing by cleanup thread with src_flushed_tmp_dir_lock
    /// Construct src_flushed_tmp_part in order to delete part with its directory at destructor
    if (auto src_part_in_memory = asInMemoryPart(src_part))
    {
        auto flushed_part_path = *src_part_in_memory->getRelativePathForPrefix(tmp_part_prefix);

        auto tmp_src_part_file_name = fs::path(tmp_dst_part_name).filename();
        src_flushed_tmp_dir_lock = src_part->storage.getTemporaryPartDirectoryHolder(tmp_src_part_file_name);

        auto flushed_part_storage = src_part_in_memory->flushToDisk(flushed_part_path, metadata_snapshot);

        src_flushed_tmp_part = MergeTreeDataPartBuilder(*this, src_part->name, flushed_part_storage)
            .withPartInfo(src_part->info)
            .withPartFormatFromDisk()
            .build();

        src_flushed_tmp_part->is_temp = true;
        src_part_storage = flushed_part_storage;
    }

    String with_copy;
    if (params.copy_instead_of_hardlink)
        with_copy = " (copying data)";

    auto dst_part_storage = src_part_storage->freeze(
        relative_data_path,
        tmp_dst_part_name,
        read_settings,
        write_settings,
        /*save_metadata_callback=*/ {},
        params);

    if (params.metadata_version_to_write.has_value())
    {
        chassert(!params.keep_metadata_version);
        auto out_metadata = dst_part_storage->writeFile(IMergeTreeDataPart::METADATA_VERSION_FILE_NAME, 4096, getContext()->getWriteSettings());
        writeText(metadata_snapshot->getMetadataVersion(), *out_metadata);
        out_metadata->finalize();
        if (getSettings()->fsync_after_insert)
            out_metadata->sync();
    }

    LOG_DEBUG(log, "Clone{} part {} to {}{}",
              src_flushed_tmp_part ? " flushed" : "",
              src_part_storage->getFullPath(),
              std::string(fs::path(dst_part_storage->getFullRootPath()) / tmp_dst_part_name),
              with_copy);

    auto dst_data_part = MergeTreeDataPartBuilder(*this, dst_part_name, dst_part_storage)
        .withPartFormatFromDisk()
        .build();

    if (!params.copy_instead_of_hardlink && params.hardlinked_files)
    {
        params.hardlinked_files->source_part_name = src_part->name;
        params.hardlinked_files->source_table_shared_id = src_part->storage.getTableSharedID();

        for (auto it = src_part->getDataPartStorage().iterate(); it->isValid(); it->next())
        {
            if (!params.files_to_copy_instead_of_hardlinks.contains(it->name())
                && it->name() != IMergeTreeDataPart::DELETE_ON_DESTROY_MARKER_FILE_NAME_DEPRECATED
                && it->name() != IMergeTreeDataPart::TXN_VERSION_METADATA_FILE_NAME)
            {
                params.hardlinked_files->hardlinks_from_source_part.insert(it->name());
            }
        }

        auto projections = src_part->getProjectionParts();
        for (const auto & [name, projection_part] : projections)
        {
            const auto & projection_storage = projection_part->getDataPartStorage();
            for (auto it = projection_storage.iterate(); it->isValid(); it->next())
            {
                auto file_name_with_projection_prefix = fs::path(projection_storage.getPartDirectory()) / it->name();
                if (!params.files_to_copy_instead_of_hardlinks.contains(file_name_with_projection_prefix)
                    && it->name() != IMergeTreeDataPart::DELETE_ON_DESTROY_MARKER_FILE_NAME_DEPRECATED
                    && it->name() != IMergeTreeDataPart::TXN_VERSION_METADATA_FILE_NAME)
                {
                    params.hardlinked_files->hardlinks_from_source_part.insert(file_name_with_projection_prefix);
                }
            }
        }
    }

    /// We should write version metadata on part creation to distinguish it from parts that were created without transaction.
    TransactionID tid = params.txn ? params.txn->tid : Tx::PrehistoricTID;
    dst_data_part->version.setCreationTID(tid, nullptr);
    dst_data_part->storeVersionMetadata();

    dst_data_part->is_temp = true;

    dst_data_part->loadColumnsChecksumsIndexes(require_part_metadata, true);
    dst_data_part->modification_time = dst_part_storage->getLastModified().epochTime();
    return std::make_pair(dst_data_part, std::move(temporary_directory_lock));
}

String MergeTreeData::getFullPathOnDisk(const DiskPtr & disk) const
{
    return disk->getPath() + relative_data_path;
}


DiskPtr MergeTreeData::tryGetDiskForDetachedPart(const String & part_name) const
{
    String additional_path = "detached/";
    const auto disks = getStoragePolicy()->getDisks();

    for (const DiskPtr & disk : disks)
        if (disk->exists(fs::path(relative_data_path) / additional_path / part_name))
            return disk;

    return nullptr;
}

DiskPtr MergeTreeData::getDiskForDetachedPart(const String & part_name) const
{
    if (auto disk = tryGetDiskForDetachedPart(part_name))
        return disk;
    throw DB::Exception(ErrorCodes::BAD_DATA_PART_NAME, "Detached part \"{}\" not found", part_name);
}


Strings MergeTreeData::getDataPaths() const
{
    Strings res;
    auto disks = getStoragePolicy()->getDisks();
    for (const auto & disk : disks)
        res.push_back(getFullPathOnDisk(disk));
    return res;
}


void MergeTreeData::reportBrokenPart(MergeTreeData::DataPartPtr & data_part) const
{
    if (data_part->getDataPartStorage().isBroken())
    {
        auto parts = getDataPartsForInternalUsage();
        LOG_WARNING(log, "Scanning parts to recover on broken disk {}@{}.", data_part->getDataPartStorage().getDiskName(), data_part->getDataPartStorage().getDiskPath());

        for (const auto & part : parts)
        {
            if (part->getDataPartStorage().getDiskName() == data_part->getDataPartStorage().getDiskName())
                broken_part_callback(part->name);
        }
    }
    else if (data_part && data_part->getState() == MergeTreeDataPartState::Active)
        broken_part_callback(data_part->name);
    else
        LOG_DEBUG(log, "Will not check potentially broken part {} because it's not active", data_part->getNameWithState());
}

MergeTreeData::MatcherFn MergeTreeData::getPartitionMatcher(const ASTPtr & partition_ast, ContextPtr local_context) const
{
    bool prefixed = false;
    String id;

    if (format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
    {
        /// Month-partitioning specific - partition value can represent a prefix of the partition to freeze.
        if (const auto * partition_lit = partition_ast->as<ASTPartition &>().value->as<ASTLiteral>())
        {
            id = partition_lit->value.getType() == Field::Types::UInt64
                 ? toString(partition_lit->value.get<UInt64>())
                 : partition_lit->value.safeGet<String>();
            prefixed = true;
        }
        else
            id = getPartitionIDFromQuery(partition_ast, local_context);
    }
    else
        id = getPartitionIDFromQuery(partition_ast, local_context);

    return [prefixed, id](const String & partition_id)
    {
        if (prefixed)
            return startsWith(partition_id, id);
        else
            return id == partition_id;
    };
}

PartitionCommandsResultInfo MergeTreeData::freezePartition(
    const ASTPtr & partition_ast,
    const StorageMetadataPtr & metadata_snapshot,
    const String & with_name,
    ContextPtr local_context,
    TableLockHolder &)
{
    return freezePartitionsByMatcher(getPartitionMatcher(partition_ast, local_context), metadata_snapshot, with_name, local_context);
}

PartitionCommandsResultInfo MergeTreeData::freezeAll(
    const String & with_name,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr local_context,
    TableLockHolder &)
{
    return freezePartitionsByMatcher([] (const String &) { return true; }, metadata_snapshot, with_name, local_context);
}

PartitionCommandsResultInfo MergeTreeData::freezePartitionsByMatcher(
    MatcherFn matcher,
    const StorageMetadataPtr & metadata_snapshot,
    const String & with_name,
    ContextPtr local_context)
{
    String clickhouse_path = fs::canonical(local_context->getPath());
    String default_shadow_path = fs::path(clickhouse_path) / "shadow/";
    fs::create_directories(default_shadow_path);
    auto increment = Increment(fs::path(default_shadow_path) / "increment.txt").get(true);

    const String shadow_path = "shadow/";

    /// Acquire a snapshot of active data parts to prevent removing while doing backup.
    const auto data_parts = getVisibleDataPartsVector(local_context);

    String backup_name = (!with_name.empty() ? escapeForFileName(with_name) : toString(increment));
    String backup_path = fs::path(shadow_path) / backup_name / "";

    for (const auto & disk : getStoragePolicy()->getDisks())
        disk->onFreeze(backup_path);

    PartitionCommandsResultInfo result;

    size_t parts_processed = 0;
    for (const auto & part : data_parts)
    {
        if (!matcher(part->info.partition_id))
            continue;

        LOG_DEBUG(log, "Freezing part {} snapshot will be placed at {}", part->name, backup_path);

        auto data_part_storage = part->getDataPartStoragePtr();
        String backup_part_path = fs::path(backup_path) / relative_data_path;

        scope_guard src_flushed_tmp_dir_lock;
        MergeTreeData::MutableDataPartPtr src_flushed_tmp_part;

        if (auto part_in_memory = asInMemoryPart(part))
        {
            auto flushed_part_path = *part_in_memory->getRelativePathForPrefix("tmp_freeze");
            src_flushed_tmp_dir_lock = part->storage.getTemporaryPartDirectoryHolder("tmp_freeze" + part->name);

            auto flushed_part_storage = part_in_memory->flushToDisk(flushed_part_path, metadata_snapshot);

            src_flushed_tmp_part = MergeTreeDataPartBuilder(*this, part->name, flushed_part_storage)
                .withPartInfo(part->info)
                .withPartFormatFromDisk()
                .build();

            src_flushed_tmp_part->is_temp = true;
            data_part_storage = flushed_part_storage;
        }

        auto callback = [this, &part, &backup_part_path](const DiskPtr & disk)
        {
            // Store metadata for replicated table.
            // Do nothing for non-replicated.
            createAndStoreFreezeMetadata(disk, part, fs::path(backup_part_path) / part->getDataPartStorage().getPartDirectory());
        };

        IDataPartStorage::ClonePartParams params
        {
            .make_source_readonly = true
        };
        auto new_storage = data_part_storage->freeze(
            backup_part_path,
            part->getDataPartStorage().getPartDirectory(),
            local_context->getReadSettings(),
            local_context->getWriteSettings(),
            callback,
            params);

        part->is_frozen.store(true, std::memory_order_relaxed);
        result.push_back(PartitionCommandResultInfo{
            .command_type = "FREEZE PART",
            .partition_id = part->info.partition_id,
            .part_name = part->name,
            .backup_path = new_storage->getFullRootPath(),
            .part_backup_path = new_storage->getFullPath(),
            .backup_name = backup_name,
        });
        ++parts_processed;
    }

    LOG_DEBUG(log, "Freezed {} parts", parts_processed);
    return result;
}

void MergeTreeData::createAndStoreFreezeMetadata(DiskPtr, DataPartPtr, String) const
{

}

PartitionCommandsResultInfo MergeTreeData::unfreezePartition(
    const ASTPtr & partition,
    const String & backup_name,
    ContextPtr local_context,
    TableLockHolder &)
{
    return unfreezePartitionsByMatcher(getPartitionMatcher(partition, local_context), backup_name, local_context);
}

PartitionCommandsResultInfo MergeTreeData::unfreezeAll(
    const String & backup_name,
    ContextPtr local_context,
    TableLockHolder &)
{
    return unfreezePartitionsByMatcher([] (const String &) { return true; }, backup_name, local_context);
}

bool MergeTreeData::removeDetachedPart(DiskPtr disk, const String & path, const String &, bool)
{
    disk->removeRecursive(path);

    return false;
}

PartitionCommandsResultInfo MergeTreeData::unfreezePartitionsByMatcher(MatcherFn matcher, const String & backup_name, ContextPtr)
{
    auto backup_path = fs::path("shadow") / escapeForFileName(backup_name) / relative_data_path;

    LOG_DEBUG(log, "Unfreezing parts by path {}", backup_path.generic_string());

    PartitionCommandsResultInfo result;

    for (const auto & disk : getStoragePolicy()->getDisks())
    {
        if (!disk->exists(backup_path))
            continue;

        for (auto it = disk->iterateDirectory(backup_path); it->isValid(); it->next())
        {
            const auto & partition_directory = it->name();

            /// Partition ID is prefix of part directory name: <partition id>_<rest of part directory name>
            auto found = partition_directory.find('_');
            if (found == std::string::npos)
                continue;
            auto partition_id = partition_directory.substr(0, found);

            if (!matcher(partition_id))
                continue;

            const auto & path = it->path();

            bool keep_shared = removeDetachedPart(disk, path, partition_directory, true);

            result.push_back(PartitionCommandResultInfo{
                .command_type = "UNFREEZE PART",
                .partition_id = partition_id,
                .part_name = partition_directory,
                .backup_path = disk->getPath() + backup_path.generic_string(),
                .part_backup_path = disk->getPath() + path,
                .backup_name = backup_name,
            });

            LOG_DEBUG(log, "Unfreezed part by path {}, keep shared data: {}", disk->getPath() + path, keep_shared);
        }
    }

    LOG_DEBUG(log, "Unfreezed {} parts", result.size());

    return result;
}

bool MergeTreeData::canReplacePartition(const DataPartPtr & src_part) const
{
    const auto settings = getSettings();

    if (!settings->enable_mixed_granularity_parts || settings->index_granularity_bytes == 0)
    {
        if (!canUseAdaptiveGranularity() && src_part->index_granularity_info.mark_type.adaptive)
            return false;
        if (canUseAdaptiveGranularity() && !src_part->index_granularity_info.mark_type.adaptive)
            return false;
    }

    return true;
}

void MergeTreeData::writePartLog(
    PartLogElement::Type type,
    const ExecutionStatus & execution_status,
    UInt64 elapsed_ns,
    const String & new_part_name,
    const DataPartPtr & result_part,
    const DataPartsVector & source_parts,
    const MergeListEntry * merge_entry)
try
{
    auto table_id = getStorageID();
    auto part_log = getContext()->getPartLog(table_id.database_name);
    if (!part_log)
        return;

    PartLogElement part_log_elem;

    part_log_elem.event_type = type;

    part_log_elem.error = static_cast<UInt16>(execution_status.code);
    part_log_elem.exception = execution_status.message;

    // construct event_time and event_time_microseconds using the same time point
    // so that the two times will always be equal up to a precision of a second.
    const auto time_now = std::chrono::system_clock::now();
    part_log_elem.event_time = timeInSeconds(time_now);
    part_log_elem.event_time_microseconds = timeInMicroseconds(time_now);

    /// TODO: Stop stopwatch in outer code to exclude ZK timings and so on
    part_log_elem.duration_ms = elapsed_ns / 1000000;

    part_log_elem.database_name = table_id.database_name;
    part_log_elem.table_name = table_id.table_name;
    part_log_elem.partition_id = MergeTreePartInfo::fromPartName(new_part_name, format_version).partition_id;

    if (result_part)
        part_log_elem.partition = result_part->partition.serializeToString(result_part->getMetadataSnapshot());
    else if (!source_parts.empty())
        part_log_elem.partition = source_parts.front()->partition.serializeToString(source_parts.front()->getMetadataSnapshot());

    part_log_elem.part_name = new_part_name;

    if (result_part)
    {
        part_log_elem.path_on_disk = result_part->getDataPartStorage().getFullPath();
        part_log_elem.bytes_compressed_on_disk = result_part->getBytesOnDisk();
        part_log_elem.rows = result_part->rows_count;
        part_log_elem.part_type = result_part->getType();
    }

    part_log_elem.source_part_names.reserve(source_parts.size());
    for (const auto & source_part : source_parts)
        part_log_elem.source_part_names.push_back(source_part->name);

    if (merge_entry)
    {
        part_log_elem.rows_read = (*merge_entry)->rows_read;
        part_log_elem.bytes_read_uncompressed = (*merge_entry)->bytes_read_uncompressed;

        part_log_elem.rows = (*merge_entry)->rows_written;
        part_log_elem.bytes_uncompressed = (*merge_entry)->bytes_written_uncompressed;
        part_log_elem.peak_memory_usage = (*merge_entry)->memory_tracker.getPeak();
    }

    part_log->add(part_log_elem);
}
catch (...)
{
    tryLogCurrentException(log, __PRETTY_FUNCTION__);
}

StorageMergeTree::PinnedPartUUIDsPtr MergeTreeData::getPinnedPartUUIDs() const
{
    std::lock_guard lock(pinned_part_uuids_mutex);
    return pinned_part_uuids;
}

MergeTreeData::CurrentlyMovingPartsTagger::CurrentlyMovingPartsTagger(MergeTreeMovingParts && moving_parts_, MergeTreeData & data_)
    : parts_to_move(std::move(moving_parts_)), data(data_)
{
    for (const auto & moving_part : parts_to_move)
        if (!data.currently_moving_parts.emplace(moving_part.part).second)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot move part '{}'. It's already moving.", moving_part.part->name);
}

MergeTreeData::CurrentlyMovingPartsTagger::~CurrentlyMovingPartsTagger()
{
    std::lock_guard lock(data.moving_parts_mutex);
    for (const auto & moving_part : parts_to_move)
    {
        /// Something went completely wrong
        if (!data.currently_moving_parts.contains(moving_part.part))
            std::terminate();
        data.currently_moving_parts.erase(moving_part.part);
    }
}

bool MergeTreeData::scheduleDataMovingJob(BackgroundJobsAssignee & assignee)
{
    if (parts_mover.moves_blocker.isCancelled())
        return false;

    auto moving_tagger = selectPartsForMove();
    if (moving_tagger->parts_to_move.empty())
        return false;

    assignee.scheduleMoveTask(ExecutableLambdaAdapter::create(
        [this, moving_tagger] () mutable
        {
            ReadSettings read_settings = Context::getGlobalContextInstance()->getReadSettings();
            WriteSettings write_settings = Context::getGlobalContextInstance()->getWriteSettings();
            return moveParts(moving_tagger, read_settings, write_settings, /* wait_for_move_if_zero_copy= */ false);
        }, moves_assignee_trigger, getStorageID()));
    return true;
}

bool MergeTreeData::areBackgroundMovesNeeded() const
{
    auto policy = getStoragePolicy();

    if (policy->getVolumes().size() > 1)
        return true;

    return policy->getVolumes().size() == 1 && policy->getVolumes()[0]->getDisks().size() > 1;
}

bool MergeTreeData::movePartsToSpace(const DataPartsVector & parts, SpacePtr space, const ReadSettings & read_settings, const WriteSettings & write_settings)
{
    if (parts_mover.moves_blocker.isCancelled())
        return false;

    auto moving_tagger = checkPartsForMove(parts, space);
    if (moving_tagger->parts_to_move.empty())
        return false;

    return moveParts(moving_tagger, read_settings, write_settings, /* wait_for_move_if_zero_copy= */ true);
}

MergeTreeData::CurrentlyMovingPartsTaggerPtr MergeTreeData::selectPartsForMove()
{
    MergeTreeMovingParts parts_to_move;

    auto can_move = [this](const DataPartPtr & part, String * reason) -> bool
    {
        if (partIsAssignedToBackgroundOperation(part))
        {
            *reason = "part already assigned to background operation.";
            return false;
        }
        if (currently_moving_parts.contains(part))
        {
            *reason = "part is already moving.";
            return false;
        }

        return true;
    };

    std::lock_guard moving_lock(moving_parts_mutex);

    parts_mover.selectPartsForMove(parts_to_move, can_move, moving_lock);
    return std::make_shared<CurrentlyMovingPartsTagger>(std::move(parts_to_move), *this);
}

MergeTreeData::CurrentlyMovingPartsTaggerPtr MergeTreeData::checkPartsForMove(const DataPartsVector & parts, SpacePtr space)
{
    std::lock_guard moving_lock(moving_parts_mutex);

    MergeTreeMovingParts parts_to_move;
    for (const auto & part : parts)
    {
        auto reservation = space->reserve(part->getBytesOnDisk());
        if (!reservation)
            throw Exception(ErrorCodes::NOT_ENOUGH_SPACE, "Move is not possible. Not enough space on '{}'", space->getName());

        auto reserved_disk = reservation->getDisk();

        if (reserved_disk->exists(relative_data_path + part->name))
            throw Exception(ErrorCodes::DIRECTORY_ALREADY_EXISTS, "Move is not possible: {} already exists",
                fullPath(reserved_disk, relative_data_path + part->name));

        if (currently_moving_parts.contains(part) || partIsAssignedToBackgroundOperation(part))
            throw Exception(ErrorCodes::PART_IS_TEMPORARILY_LOCKED,
                            "Cannot move part '{}' because it's participating in background process", part->name);

        parts_to_move.emplace_back(part, std::move(reservation));
    }
    return std::make_shared<CurrentlyMovingPartsTagger>(std::move(parts_to_move), *this);
}

bool MergeTreeData::moveParts(const CurrentlyMovingPartsTaggerPtr & moving_tagger, const ReadSettings & read_settings, const WriteSettings & write_settings, [[maybe_unused]] bool wait_for_move_if_zero_copy)
{
    LOG_INFO(log, "Got {} parts to move.", moving_tagger->parts_to_move.size());

    const auto settings = getSettings();

    bool result = true;
    for (const auto & moving_part : moving_tagger->parts_to_move)
    {
        Stopwatch stopwatch;
        MutableDataPartPtr cloned_part;

        auto write_part_log = [&](const ExecutionStatus & execution_status)
        {
            writePartLog(
                PartLogElement::Type::MOVE_PART,
                execution_status,
                stopwatch.elapsed(),
                moving_part.part->name,
                cloned_part,
                {moving_part.part},
                nullptr);
        };

        try
        {
            /// If zero-copy replication enabled than replicas shouldn't try to
            /// move parts to another disk simultaneously. For this purpose we
            /// use shared lock across replicas. NOTE: it's not 100% reliable,
            /// because we are not checking lock while finishing part move.
            /// However it's not dangerous at all, we will just have very rare
            /// copies of some part.
            ///
            /// FIXME: this code is related to Replicated merge tree, and not
            /// common for ordinary merge tree. So it's a bad design and should
            /// be fixed.
            auto disk = moving_part.reserved_space->getDisk();
            if (supportsReplication() && disk->supportZeroCopyReplication() && settings->allow_remote_fs_zero_copy_replication)
            {
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Zero copy replication is not implemented");
#if 0
                /// If we acuqired lock than let's try to move. After one
                /// replica will actually move the part from disk to some
                /// zero-copy storage other replicas will just fetch
                /// metainformation.
                if (auto lock = tryCreateZeroCopyExclusiveLock(moving_part.part->name, disk); lock)
                {
                    cloned_part = parts_mover.clonePart(moving_part, read_settings, write_settings);
                    parts_mover.swapClonedPart(cloned_part);
                }
                else
                {
                    /// Move will be retried but with backoff.
                    LOG_DEBUG(log, "Move of part {} postponed, because zero copy mode enabled and someone other moving this part right now", moving_part.part->name);
                    result = false;
                    continue;
                }
#endif
            }
            else /// Ordinary move as it should be
            {
                cloned_part = parts_mover.clonePart(moving_part, read_settings, write_settings);
                parts_mover.swapClonedPart(cloned_part);
            }
            write_part_log({});
        }
        catch (...)
        {
            write_part_log(ExecutionStatus::fromCurrentException());
            if (cloned_part)
                cloned_part->remove();

            throw;
        }
    }
    return result;
}

bool MergeTreeData::partsContainSameProjections(const DataPartPtr & left, const DataPartPtr & right)
{
    if (left->getProjectionParts().size() != right->getProjectionParts().size())
        return false;
    for (const auto & [name, _] : left->getProjectionParts())
    {
        if (!right->hasProjection(name))
            return false;
    }
    return true;
}

bool MergeTreeData::canUsePolymorphicParts() const
{
    return canUsePolymorphicParts(*getSettings(), nullptr);
}

bool MergeTreeData::canUsePolymorphicParts(const MergeTreeSettings & settings, String * out_reason) const
{
    if (!canUseAdaptiveGranularity())
    {
        if (out_reason && (settings.min_rows_for_wide_part != 0 || settings.min_bytes_for_wide_part != 0
            || settings.min_rows_for_compact_part != 0 || settings.min_bytes_for_compact_part != 0))
        {
            *out_reason = fmt::format(
                    "Stream can't create parts with adaptive granularity, but settings"
                    " min_rows_for_wide_part = {}"
                    ", min_bytes_for_wide_part = {}"
                    ", min_rows_for_compact_part = {}"
                    ", min_bytes_for_compact_part = {}"
                    ". Parts with non-adaptive granularity can be stored only in Wide (default) format.",
                    settings.min_rows_for_wide_part,    settings.min_bytes_for_wide_part,
                    settings.min_rows_for_compact_part, settings.min_bytes_for_compact_part);
        }

        return false;
    }

    return true;
}

AlterConversionsPtr MergeTreeData::getAlterConversionsForPart(MergeTreeDataPartPtr part) const
{
    auto commands = getAlterMutationCommandsForPart(part);
    auto result = std::make_shared<AlterConversions>();

    for (const auto & command : commands | std::views::reverse)
        result->addMutationCommand(command);

    return result;
}

MergeTreeData::WriteAheadLogPtr MergeTreeData::getWriteAheadLog()
{
    std::lock_guard lock(write_ahead_log_mutex);
    if (!write_ahead_log)
    {
        auto reservation = reserveSpace(getSettings()->write_ahead_log_max_bytes);
        write_ahead_log = std::make_shared<MergeTreeWriteAheadLog>(*this, reservation->getDisk());
    }

    return write_ahead_log;
}

NamesAndTypesList MergeTreeData::getVirtuals() const
{
    return NamesAndTypesList{
        NameAndTypePair("_part", std::make_shared<DataTypeString>()),
        NameAndTypePair("_part_index", std::make_shared<DataTypeUInt64>()),
        NameAndTypePair("_part_uuid", std::make_shared<DataTypeUUID>()),
        NameAndTypePair("_partition_id", std::make_shared<DataTypeString>()),
        NameAndTypePair("_partition_value", getPartitionValueType()),
        NameAndTypePair("_sample_factor", std::make_shared<DataTypeFloat64>()),
        NameAndTypePair("_part_offset", std::make_shared<DataTypeUInt64>()),
        /// proton: starts.
        NameAndTypePair(ProtonConsts::RESERVED_SHARD, std::make_shared<DataTypeInt32>()),
        /// proton: ends.
        LightweightDeleteDescription::FILTER_COLUMN,
    };
}

size_t MergeTreeData::getTotalMergesWithTTLInMergeList() const
{
    return getContext()->getMergeList().getMergesWithTTLCount();
}

void MergeTreeData::addPartContributionToDataVolume(const DataPartPtr & part)
{
    increaseDataVolume(part->getBytesOnDisk(), part->rows_count, 1);
}

void MergeTreeData::removePartContributionToDataVolume(const DataPartPtr & part)
{
    increaseDataVolume(-part->getBytesOnDisk(), -part->rows_count, -1);
}

void MergeTreeData::increaseDataVolume(ssize_t bytes, ssize_t rows, ssize_t parts)
{
    total_active_size_bytes.fetch_add(bytes, std::memory_order_acq_rel);
    total_active_size_rows.fetch_add(rows, std::memory_order_acq_rel);
    total_active_size_parts.fetch_add(parts, std::memory_order_acq_rel);
}

void MergeTreeData::setDataVolume(size_t bytes, size_t rows, size_t parts)
{
    total_active_size_bytes.store(bytes, std::memory_order_release);
    total_active_size_rows.store(rows, std::memory_order_release);
    total_active_size_parts.store(parts, std::memory_order_release);
}

bool MergeTreeData::insertQueryIdOrThrow(const String & query_id, size_t max_queries) const
{
    std::lock_guard lock(query_id_set_mutex);
    return insertQueryIdOrThrowNoLock(query_id, max_queries);
}

bool MergeTreeData::insertQueryIdOrThrowNoLock(const String & query_id, size_t max_queries) const
{
    if (query_id_set.contains(query_id))
        return false;
    if (query_id_set.size() >= max_queries)
        throw Exception(
            ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES,
            "Too many simultaneous queries for table {}. Maximum is: {}",
            log.loadName(),
            max_queries);
    query_id_set.insert(query_id);
    return true;
}

void MergeTreeData::removeQueryId(const String & query_id) const
{
    std::lock_guard lock(query_id_set_mutex);
    removeQueryIdNoLock(query_id);
}

void MergeTreeData::removeQueryIdNoLock(const String & query_id) const
{
    if (!query_id_set.contains(query_id))
        LOG_WARNING(log, "We have query_id removed but it's not recorded. This is a bug");
    else
        query_id_set.erase(query_id);
}

std::shared_ptr<QueryIdHolder> MergeTreeData::getQueryIdHolder(const String & query_id, UInt64 max_concurrent_queries) const
{
    auto lock = std::lock_guard<std::mutex>(query_id_set_mutex);
    if (insertQueryIdOrThrowNoLock(query_id, max_concurrent_queries))
    {
        try
        {
            return std::make_shared<QueryIdHolder>(query_id, *this);
        }
        catch (...)
        {
            /// If we fail to construct the holder, remove query_id explicitly to avoid leak.
            removeQueryIdNoLock(query_id);
            throw;
        }
    }
    return nullptr;
}

ReservationPtr MergeTreeData::balancedReservation(
    const StorageMetadataPtr & metadata_snapshot,
    size_t part_size,
    size_t max_volume_index,
    const String & part_name,
    const MergeTreePartInfo & part_info,
    MergeTreeData::DataPartsVector covered_parts,
    std::optional<CurrentlySubmergingEmergingTagger> * tagger_ptr,
    const IMergeTreeDataPart::TTLInfos * ttl_infos,
    bool is_insert)
{
    ReservationPtr reserved_space;
    auto min_bytes_to_rebalance_partition_over_jbod = getSettings()->min_bytes_to_rebalance_partition_over_jbod;
    if (tagger_ptr && min_bytes_to_rebalance_partition_over_jbod > 0 && part_size >= min_bytes_to_rebalance_partition_over_jbod)
    {
        try
        {
            const auto & disks = getStoragePolicy()->getVolume(max_volume_index)->getDisks();
            std::map<String, size_t> disk_occupation;
            std::map<String, std::vector<String>> disk_parts_for_logging;
            for (const auto & disk : disks)
                disk_occupation.emplace(disk->getName(), 0);

            std::set<String> committed_big_parts_from_partition;
            std::set<String> submerging_big_parts_from_partition;
            std::lock_guard lock(currently_submerging_emerging_mutex);

            for (const auto & part : currently_submerging_big_parts)
            {
                if (part_info.partition_id == part->info.partition_id)
                    submerging_big_parts_from_partition.insert(part->name);
            }

            {
                auto lock_parts = lockParts();
                if (covered_parts.empty())
                {
                    // It's a part fetch. Calculate `covered_parts` here.
                    MergeTreeData::DataPartPtr covering_part;
                    covered_parts = getActivePartsToReplace(part_info, part_name, covering_part, lock_parts);
                }

                // Remove irrelevant parts.
                std::erase_if(covered_parts,
                        [min_bytes_to_rebalance_partition_over_jbod](const auto & part)
                        {
                            return !(part->isStoredOnDisk() && part->getBytesOnDisk() >= min_bytes_to_rebalance_partition_over_jbod);
                        });

                // Include current submerging big parts which are not yet in `currently_submerging_big_parts`
                for (const auto & part : covered_parts)
                    submerging_big_parts_from_partition.insert(part->name);

                for (const auto & part : getDataPartsStateRange(MergeTreeData::DataPartState::Active))
                {
                    if (part->isStoredOnDisk() && part->getBytesOnDisk() >= min_bytes_to_rebalance_partition_over_jbod
                        && part_info.partition_id == part->info.partition_id)
                    {
                        auto name = part->getDataPartStorage().getDiskName();
                        auto it = disk_occupation.find(name);
                        if (it != disk_occupation.end())
                        {
                            if (!submerging_big_parts_from_partition.contains(part->name))
                            {
                                it->second += part->getBytesOnDisk();
                                disk_parts_for_logging[name].push_back(formatReadableSizeWithBinarySuffix(part->getBytesOnDisk()));
                                committed_big_parts_from_partition.insert(part->name);
                            }
                            else
                            {
                                disk_parts_for_logging[name].push_back(formatReadableSizeWithBinarySuffix(part->getBytesOnDisk()) + " (submerging)");
                            }
                        }
                        else
                        {
                            // Part is on different volume. Ignore it.
                        }
                    }
                }
            }

            for (const auto & [name, emerging_part] : currently_emerging_big_parts)
            {
                // It's possible that the emerging big parts are committed and get added twice. Thus a set is used to deduplicate.
                if (!committed_big_parts_from_partition.contains(name)
                    && part_info.partition_id == emerging_part.partition_id)
                {
                    auto it = disk_occupation.find(emerging_part.disk_name);
                    if (it != disk_occupation.end())
                    {
                        it->second += emerging_part.estimate_bytes;
                        disk_parts_for_logging[emerging_part.disk_name].push_back(
                            formatReadableSizeWithBinarySuffix(emerging_part.estimate_bytes) + " (emerging)");
                    }
                    else
                    {
                        // Part is on different volume. Ignore it.
                    }
                }
            }

            size_t min_occupation_size = std::numeric_limits<size_t>::max();
            std::vector<String> candidates;
            for (const auto & [disk_name, size] : disk_occupation)
            {
                if (size < min_occupation_size)
                {
                    min_occupation_size = size;
                    candidates = {disk_name};
                }
                else if (size == min_occupation_size)
                {
                    candidates.push_back(disk_name);
                }
            }

            if (!candidates.empty())
            {
                // Random pick one disk from best candidates
                std::shuffle(candidates.begin(), candidates.end(), thread_local_rng);
                String selected_disk_name = candidates.front();
                WriteBufferFromOwnString log_str;
                writeCString("\nbalancer: \n", log_str);
                for (const auto & [disk_name, per_disk_parts] : disk_parts_for_logging)
                    writeString(fmt::format("  {}: [{}]\n", disk_name, fmt::join(per_disk_parts, ", ")), log_str);
                LOG_DEBUG(log, fmt::runtime(log_str.str()));

                if (ttl_infos)
                    reserved_space = tryReserveSpacePreferringTTLRules(
                        metadata_snapshot,
                        part_size,
                        *ttl_infos,
                        time(nullptr),
                        max_volume_index,
                        is_insert,
                        getStoragePolicy()->getDiskByName(selected_disk_name));
                else
                    reserved_space = tryReserveSpace(part_size, getStoragePolicy()->getDiskByName(selected_disk_name));

                if (reserved_space)
                {
                    currently_emerging_big_parts.emplace(
                        part_name, EmergingPartInfo{reserved_space->getDisk(0)->getName(), part_info.partition_id, part_size});

                    for (const auto & part : covered_parts)
                    {
                        if (currently_submerging_big_parts.contains(part))
                            LOG_WARNING(log, "currently_submerging_big_parts contains duplicates. JBOD might lose balance");
                        else
                            currently_submerging_big_parts.insert(part);
                    }

                    // Record submerging big parts in the tagger to clean them up.
                    tagger_ptr->emplace(*this, part_name, std::move(covered_parts), log.load());
                }
            }
        }
        catch (...)
        {
            LOG_DEBUG(log, "JBOD balancer encounters an error. Fallback to random disk selection");
            tryLogCurrentException(log);
        }
    }
    return reserved_space;
}

StorageSnapshotPtr MergeTreeData::getStorageSnapshot(const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) const
{
    auto snapshot_data = std::make_unique<SnapshotData>();

    {
        auto lock = lockParts();
        snapshot_data->parts = getVisibleDataPartsVectorUnlocked(query_context, lock);
    }

    snapshot_data->alter_conversions.reserve(snapshot_data->parts.size());
    for (const auto & part : snapshot_data->parts)
        snapshot_data->alter_conversions.push_back(getAlterConversionsForPart(part));

    return std::make_shared<StorageSnapshot>(*this, metadata_snapshot, std::move(snapshot_data));  /// proton: updates
}

StorageSnapshotPtr MergeTreeData::getStorageSnapshotWithoutData(const StorageMetadataPtr & metadata_snapshot, ContextPtr) const
{
    auto lock = lockParts();
    return std::make_shared<StorageSnapshot>(*this, metadata_snapshot, std::make_unique<SnapshotData>());  /// proton: updates
}

void MergeTreeData::incrementInsertedPartsProfileEvent(MergeTreeDataPartType type)
{
    switch (type.getValue())
    {
        case MergeTreeDataPartType::Wide:
            ProfileEvents::increment(ProfileEvents::InsertedWideParts);
            break;
        case MergeTreeDataPartType::Compact:
            ProfileEvents::increment(ProfileEvents::InsertedCompactParts);
            break;
        case MergeTreeDataPartType::InMemory:
            ProfileEvents::increment(ProfileEvents::InsertedInMemoryParts);
            break;
        default:
            break;
    }
}

void MergeTreeData::incrementMergedPartsProfileEvent(MergeTreeDataPartType type)
{
    switch (type.getValue())
    {
        case MergeTreeDataPartType::Wide:
            ProfileEvents::increment(ProfileEvents::MergedIntoWideParts);
            break;
        case MergeTreeDataPartType::Compact:
            ProfileEvents::increment(ProfileEvents::MergedIntoCompactParts);
            break;
        case MergeTreeDataPartType::InMemory:
            ProfileEvents::increment(ProfileEvents::MergedIntoInMemoryParts);
            break;
        default:
            break;
    }
}

MergeTreeData::MutableDataPartPtr MergeTreeData::createEmptyPart(
    MergeTreePartInfo & new_part_info, const MergeTreePartition & partition, const String & new_part_name,
    const MergeTreeTransactionPtr & txn)
{
    auto metadata_snapshot = getInMemoryMetadataPtr();
    auto settings = getSettings();

    auto block = metadata_snapshot->getSampleBlock();
    NamesAndTypesList columns = metadata_snapshot->getColumns().getAllPhysical().filter(block.getNames());

    auto minmax_idx = std::make_shared<IMergeTreeDataPart::MinMaxIndex>();
    minmax_idx->update(block, getMinMaxColumnsNames(metadata_snapshot->getPartitionKey()));

    DB::IMergeTreeDataPart::TTLInfos move_ttl_infos;
    VolumePtr volume = getStoragePolicy()->getVolume(0);
    ReservationPtr reservation = reserveSpacePreferringTTLRules(metadata_snapshot, 0, move_ttl_infos, time(nullptr), 0, true);
    VolumePtr data_part_volume = createVolumeFromReservation(reservation, volume);

    auto new_data_part = getDataPartBuilder(new_part_name, data_part_volume, EMPTY_PART_TMP_PREFIX + new_part_name)
        .withBytesAndRowsOnDisk(0, 0)
        .withPartInfo(new_part_info)
        .build();

    if (settings->assign_part_uuids)
        new_data_part->uuid = UUIDHelpers::generateV4();

    new_data_part->setColumns(columns, {}, metadata_snapshot->getMetadataVersion());
    new_data_part->rows_count = block.rows();

    new_data_part->partition = partition;

    new_data_part->minmax_idx = std::move(minmax_idx);
    new_data_part->is_temp = true;

    auto new_data_part_storage = new_data_part->getDataPartStoragePtr();
    new_data_part_storage->beginTransaction();

    SyncGuardPtr sync_guard;
    if (new_data_part->isStoredOnDisk())
    {
        /// The name could be non-unique in case of stale files from previous runs.
        if (new_data_part_storage->exists())
        {
            /// The path has to be unique, all tmp directories are deleted at startup in case of stale files from previous runs.
            /// New part have to capture its name, therefore there is no concurrentcy in directory creation
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "New empty part is about to matirialize but the dirrectory already exist"
                            ", new part {}"
                            ", directory {}",
                            new_part_name, new_data_part_storage->getFullPath());
        }

        new_data_part_storage->createDirectories();

        if (getSettings()->fsync_part_directory)
            sync_guard = new_data_part_storage->getDirectorySyncGuard();
    }

    /// This effectively chooses minimal compression method:
    ///  either default lz4 or compression method with zero thresholds on absolute and relative part size.
    auto compression_codec = getContext()->chooseCompressionCodec(0, 0);

    const auto & index_factory = MergeTreeIndexFactory::instance();
    MergedBlockOutputStream out(new_data_part, metadata_snapshot, columns,
        index_factory.getMany(metadata_snapshot->getSecondaryIndices()),
        compression_codec, txn ? txn->tid : Tx::PrehistoricTID);

    bool sync_on_insert = settings->fsync_after_insert;

    out.write(block);
    /// Here is no projections as no data inside
    out.finalizePart(new_data_part, sync_on_insert);

    new_data_part_storage->precommitTransaction();
    return new_data_part;
}

CurrentlySubmergingEmergingTagger::~CurrentlySubmergingEmergingTagger()
{
    std::lock_guard lock(storage.currently_submerging_emerging_mutex);

    for (const auto & part : submerging_parts)
    {
        if (!storage.currently_submerging_big_parts.contains(part))
        {
            LOG_ERROR(log, "currently_submerging_big_parts doesn't contain part {} to erase. This is a bug", part->name);
            assert(false);
        }
        else
            storage.currently_submerging_big_parts.erase(part);
    }
    storage.currently_emerging_big_parts.erase(emerging_part_name);
}

}
