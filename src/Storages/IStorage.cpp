#include <Storages/IStorage.h>

#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ParserTablesInSelectQuery.h>
#include <Parsers/parseQuery.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/AlterCommands.h>
#include <Common/StringUtils/StringUtils.h>

/// proton: starts.
#include <Interpreters/MetadataHelper.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Storages/SchemaBlock.h>
#include <Storages/storageUtil.h>

#include <ranges>
#include <fmt/ranges.h>
/// proton: ends.

namespace DB
{
namespace ErrorCodes
{
    extern const int STREAM_IS_DROPPED;
    extern const int NOT_IMPLEMENTED;
    extern const int DEADLOCK_AVOIDED;
    /// proton: starts.
    extern const int HAVE_DEPENDENT_OBJECTS;
    extern const int RESOURCE_NOT_FOUND;
    /// proton: ends.
}

bool IStorage::isVirtualColumn(const String & column_name, const StorageMetadataPtr & metadata_snapshot) const
{
    /// Virtual column maybe overridden by real column
    return !metadata_snapshot->getColumns().has(column_name) && getVirtuals().contains(column_name);
}

RWLockImpl::LockHolder IStorage::tryLockTimed(
    const RWLock & rwlock, RWLockImpl::Type type, const String & query_id, const std::chrono::milliseconds & acquire_timeout) const
{
    auto lock_holder = rwlock->getLock(type, query_id, acquire_timeout);
    if (!lock_holder)
    {
        const String type_str = type == RWLockImpl::Type::Read ? "READ" : "WRITE";
        throw Exception(ErrorCodes::DEADLOCK_AVOIDED,
            "{} locking attempt on \"{}\" has timed out! ({}ms) Possible deadlock avoided. Client should retry.",
            type_str, getStorageID(), acquire_timeout.count());
    }
    return lock_holder;
}

TableLockHolder IStorage::lockForShare(const String & query_id, const std::chrono::milliseconds & acquire_timeout)
{
    TableLockHolder result = tryLockTimed(drop_lock, RWLockImpl::Read, query_id, acquire_timeout);

    if (is_dropped || is_detached)
    {
        auto table_id = getStorageID();
        throw Exception(ErrorCodes::STREAM_IS_DROPPED, "Stream {}.{} is dropped", table_id.database_name, table_id.table_name);
    }
    return result;
}

TableLockHolder IStorage::tryLockForShare(const String & query_id, const std::chrono::milliseconds & acquire_timeout)
{
    TableLockHolder result = tryLockTimed(drop_lock, RWLockImpl::Read, query_id, acquire_timeout);

    if (is_dropped)
    {
        // Table was dropped while acquiring the lock
        result = nullptr;
    }

    return result;
}

IStorage::AlterLockHolder IStorage::lockForAlter(const std::chrono::milliseconds & acquire_timeout)
{
    AlterLockHolder lock{alter_lock, std::defer_lock};

    if (!lock.try_lock_for(acquire_timeout))
        throw Exception(ErrorCodes::DEADLOCK_AVOIDED,
                        "Locking attempt for ALTER on \"{}\" has timed out! ({} ms) "
                        "Possible deadlock avoided. Client should retry.",
                        getStorageID().getFullTableName(), acquire_timeout.count());

    if (is_dropped || is_detached)
        throw Exception(ErrorCodes::STREAM_IS_DROPPED, "Stream {} is dropped", getStorageID());

    return lock;
}


TableExclusiveLockHolder IStorage::lockExclusively(const String & query_id, const std::chrono::milliseconds & acquire_timeout)
{
    TableExclusiveLockHolder result;
    result.drop_lock = tryLockTimed(drop_lock, RWLockImpl::Write, query_id, acquire_timeout);

    if (is_dropped || is_detached)
        throw Exception(ErrorCodes::STREAM_IS_DROPPED, "Stream {} is dropped", getStorageID());

    return result;
}

Pipe IStorage::watch(
    const Names & /*column_names*/,
    const SelectQueryInfo & /*query_info*/,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum & /*processed_stage*/,
    size_t /*max_block_size*/,
    size_t /*num_streams*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method watch is not supported by storage {}", getName());
}

Pipe IStorage::read(
    const Names & /*column_names*/,
    const StorageSnapshotPtr & /*storage_snapshot*/,
    SelectQueryInfo & /*query_info*/,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    size_t /*num_streams*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method read is not supported by storage {}", getName());
}

void IStorage::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    auto pipe = read(column_names, storage_snapshot, query_info, context, processed_stage, max_block_size, num_streams);

    /// parallelize processing if not yet
    const size_t output_ports = pipe.numOutputPorts();
    const bool parallelize_output = context->getSettingsRef().parallelize_output_from_storages;
    if (parallelize_output && parallelizeOutputAfterReading(context) && output_ports > 0 && output_ports < num_streams)
        pipe.resize(num_streams);

    readFromPipe(query_plan, std::move(pipe), column_names, storage_snapshot, query_info, context, getName());
}

void IStorage::readFromPipe(
    QueryPlan & query_plan,
    Pipe pipe,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    std::string storage_name)
{
    if (pipe.empty())
    {
        auto header = storage_snapshot->getSampleBlockForColumns(column_names);
        InterpreterSelectQuery::addEmptySourceToQueryPlan(query_plan, header, query_info, context);
    }
    else
    {
        auto read_step = std::make_unique<ReadFromStorageStep>(std::move(pipe), storage_name, query_info.storage_limits);
        query_plan.addStep(std::move(read_step));
    }
}

std::optional<QueryPipeline> IStorage::distributedWrite(
    const ASTInsertQuery & /*query*/,
    ContextPtr /*context*/)
{
    return {};
}

Pipe IStorage::alterPartition(
    const StorageMetadataPtr & /* metadata_snapshot */, const PartitionCommands & /* commands */, ContextPtr /* context */)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Partition operations are not supported by storage {}", getName());
}

void IStorage::alter(const AlterCommands & params, ContextPtr context, AlterLockHolder &)
{
    auto table_id = getStorageID();
    StorageInMemoryMetadata new_metadata = getInMemoryMetadata();
    params.apply(new_metadata, context);
    /// proton: starts. StorageView::alter will use this function
    if (params.empty())
        DatabaseCatalog::instance().getDatabase(table_id.database_name)->alterTable(context, table_id, new_metadata);
    else
        DatabaseCatalog::instance().getDatabase(table_id.database_name)->alterTable(context, table_id, new_metadata, params.front().typeString(), params.astCommands(table_id));
    /// proton: ends.
    setInMemoryMetadata(new_metadata);
}

void IStorage::checkAlterIsPossible(const AlterCommands & commands, ContextPtr /* context */) const
{
    for (const auto & command : commands)
    {
        if (!command.isCommentAlter())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Alter of type '{}' is not supported by storage {}",
                command.type, getName());
    }
}

void IStorage::checkMutationIsPossible(const MutationCommands & /*commands*/, const Settings & /*settings*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The current engine {} doesn't support mutations", getName());
}

void IStorage::checkAlterPartitionIsPossible(
    const PartitionCommands & /*commands*/, const StorageMetadataPtr & /*metadata_snapshot*/, const Settings & /*settings*/) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The current engine {} doesn't support partitioning", getName());
}

StorageID IStorage::getStorageID() const
{
    std::lock_guard lock(id_mutex);
    return storage_id;
}

void IStorage::renameInMemory(const StorageID & new_table_id)
{
    std::lock_guard lock(id_mutex);
    storage_id = new_table_id;
}

NamesAndTypesList IStorage::getVirtuals() const
{
    return {};
}

Names IStorage::getAllRegisteredNames() const
{
    Names result;
    auto getter = [](const auto & column) { return column.name; };
    const NamesAndTypesList & available_columns = getInMemoryMetadata().getColumns().getAllPhysical();
    std::transform(available_columns.begin(), available_columns.end(), std::back_inserter(result), getter);
    return result;
}

NameDependencies IStorage::getDependentViewsByColumn(ContextPtr context) const
{
    NameDependencies name_deps;
    auto dependencies = DatabaseCatalog::instance().getDependencies(storage_id);
    for (const auto & depend_id : dependencies)
    {
        auto depend_table = DatabaseCatalog::instance().getTable(depend_id, context);
        auto metadata_snapshot = depend_table->getInMemoryMetadataPtr();
        if (metadata_snapshot->getSelectQuery().inner_query)
        {
            /// proton: starts. MV supported union query for now
            /// FIXME: Need to check membership of required columns
            const auto & select_with_union_query = metadata_snapshot->getSelectQuery().inner_query;
            for (const auto & select_query : select_with_union_query->as<ASTSelectWithUnionQuery&>().list_of_selects->children)
            {
                auto required_columns = InterpreterSelectQuery(select_query, context, SelectQueryOptions{}.noModify()).getRequiredColumns();
                for (const auto & col_name : required_columns)
                    name_deps[col_name].push_back(depend_id.table_name);
            }
            /// proton: ends.
        }
    }
    return name_deps;
}

bool IStorage::isStaticStorage() const
{
    auto storage_policy = getStoragePolicy();
    if (storage_policy)
    {
        for (const auto & disk : storage_policy->getDisks())
            if (!(disk->isReadOnly() || disk->isWriteOnce()))
                return false;
        return true;
    }
    return false;
}

BackupEntries IStorage::backup(const ASTs &, ContextPtr)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The engine {} doesn't support backups", getName());
}

RestoreDataTasks IStorage::restoreFromBackup(const BackupPtr &, const String &, const ASTs &, ContextMutablePtr)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The engine {} doesn't support restoring", getName());
}

std::string PrewhereInfo::dump() const
{
    WriteBufferFromOwnString ss;
    ss << "PrewhereDagInfo\n";

    if (row_level_filter)
    {
        ss << "row_level_filter " << row_level_filter->dumpDAG() << "\n";
    }

    if (prewhere_actions)
    {
        ss << "prewhere_actions " << prewhere_actions->dumpDAG() << "\n";
    }

    ss << "remove_prewhere_column " << remove_prewhere_column
       << ", need_filter " << need_filter << "\n";

    return ss.str();
}

std::string FilterDAGInfo::dump() const
{
    WriteBufferFromOwnString ss;
    ss << "FilterDAGInfo for column '" << column_name <<"', do_remove_column "
       << do_remove_column << "\n";
    if (actions)
    {
        ss << "actions " << actions->dumpDAG() << "\n";
    }

    return ss.str();
}

/// proton: starts.
void IStorage::checkTableCanBeDropped(ContextPtr context) const
{
    if (context->getSettingsRef().enable_dependency_check.value)
    {
        if (const auto & dependencies = DatabaseCatalog::instance().getDependencies(getStorageID()); !dependencies.empty())
        {
            const auto & dependencies_views = dependencies | std::views::transform([](const auto & elem) { return elem.getNameForLogs(); });
            throw Exception(
                ErrorCodes::HAVE_DEPENDENT_OBJECTS,
                "Cannot drop '{}', because some views depend on it: {}",
                getStorageID().getNameForLogs(),
                fmt::join(dependencies_views, ", "));
        }
    }
}

void IStorage::checkTableCanBeRenamed(ContextPtr context) const
{
    if (context->getSettingsRef().enable_dependency_check.value)
    {
        if (const auto & dependencies = DatabaseCatalog::instance().getDependencies(getStorageID()); !dependencies.empty())
        {
            const auto & dependencies_views = dependencies | std::views::transform([](const auto & elem) { return elem.getNameForLogs(); });
            throw Exception(
                ErrorCodes::HAVE_DEPENDENT_OBJECTS,
                "Cannot rename '{}', because some views depend on it: {}",
                getStorageID().getNameForLogs(),
                fmt::join(dependencies_views, ", "));
        }
    }
}

void IStorage::setInMemoryMetadata(const StorageInMemoryMetadata & metadata_)
{
    /// FIXME: Sanity check whether the version already exists
    /// No check yet due to partial support multiple versions
    StorageMetadataPtr updated;
    {
        std::unique_lock lock(multi_metadata_mutex);
        metadata.set(std::make_unique<StorageInMemoryMetadata>(metadata_));

        updated = metadata.get();
        multi_version_metadata.emplace(updated->getVersion(), std::make_shared<SchemaBlock>(updated, updated->getSampleBlock()));
        /// Special 'ANY_SCHEMA_VERSION' always use latest metadata/schema
        multi_version_metadata.insert_or_assign(
            cluster::SchemaRecord::ANY_SCHEMA_VERSION, std::make_shared<SchemaBlock>(updated, updated->getSampleBlock()));
    }

    onInMemoryMetadataUpdate(updated);
}

StorageMetadataPtr IStorage::getInMemoryMetadataByVersion(UInt16 version) const { return getMetadataByVersion(version)->storage_metadata; }

const Block & IStorage::getSchemaByVersion(UInt16 version) const { return getMetadataByVersion(version)->schema_block; }

SchemaBlockPtr IStorage::getMetadataByVersion(UInt16 version) const
{
    {
        std::shared_lock lock(multi_metadata_mutex);
        auto iter = multi_version_metadata.find(version);
        if (iter != multi_version_metadata.end())
            return iter->second;
    }

    /// Try load multi version metadata from metastore, and then find the version again
    if (!loaded_from_metastore.test())
    {
        auto loaded_metadata_vec = getMultiVersionMetadataFromMetastore(shared_from_this(), Context::getGlobalContextInstance());

        std::unique_lock lock(multi_metadata_mutex);
        for (auto & loaded_metadata : loaded_metadata_vec)
            multi_version_metadata.emplace(
                loaded_metadata->getVersion(), std::make_shared<SchemaBlock>(loaded_metadata, loaded_metadata->getSampleBlock()));

        loaded_from_metastore.test_and_set();

        auto iter = multi_version_metadata.find(version);
        if (iter != multi_version_metadata.end())
            return iter->second;
    }

    throw Exception(
        ErrorCodes::RESOURCE_NOT_FOUND, "Metadata for version={} of '{}' not found", version, getStorageID().getFullTableName());
}

std::optional<Block>
IStorage::getWriteSampleBlock(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context)
{
    if (query)
    {
        if (const auto * insert = query->as<ASTInsertQuery>(); insert != nullptr)
        {
            InterpreterInsertQuery interpreter{query, local_context};
            return interpreter.getSampleBlock(*insert, shared_from_this(), metadata_snapshot);
        }
    }
    else if (local_context->isQueryFromMaterializedView())
    {
        ParserTableExpression parser;
        /// The creator is the MV's name.
        auto parsed_ast = parseQuery(parser, local_context->getCreator(), 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
        if (const auto * table_expression = parsed_ast->as<ASTTableExpression>(); table_expression != nullptr)
        {
            StorageID mv_id{table_expression->database_and_table_name};
            if (const auto table_ptr = DatabaseCatalog::instance().getTable(mv_id, local_context))
            {
                /// Exclude the columns which are not in MV's output.
                const auto storage_sample_block = metadata_snapshot->getSampleBlock();
                const auto mv_sample_block = table_ptr->getInMemoryMetadataPtr()->getSampleBlock();

                Block res;
                res.reserve(std::min(storage_sample_block.columns(), mv_sample_block.columns()));
                for (const auto & col : storage_sample_block.getColumnsWithTypeAndName())
                {
                    if (mv_sample_block.has(col.name))
                        res.insert(col);
                }

                return res;
            }
        }
    }

    return std::nullopt;
}
/// proton: ends.

}
