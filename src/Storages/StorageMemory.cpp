#include <cassert>
#include <Common/Exception.h>

#include <Interpreters/MutationsInterpreter.h>
#include <Interpreters/getColumnFromBlock.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <Storages/AlterCommands.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageMemory.h>
#include <Storages/MemorySettings.h>
#include <DataTypes/ObjectUtils.h>
#include <Columns/ColumnObject.h>

#include <IO/WriteHelpers.h>
#include <Processors/ISource.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Parsers/ASTCreateQuery.h>


namespace DB
{
namespace MemorySetting
{
    extern const MemorySettingsBool compress;
    extern const MemorySettingsUInt64 max_bytes_to_keep;
    extern const MemorySettingsUInt64 max_rows_to_keep;
    extern const MemorySettingsUInt64 min_bytes_to_keep;
    extern const MemorySettingsUInt64 min_rows_to_keep;
}

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int CANNOT_RESTORE_TABLE;
    extern const int NOT_IMPLEMENTED;
}


class MemorySource : public ISource
{
    using InitializerFunc = std::function<void(std::shared_ptr<const Blocks> &)>;
public:

    MemorySource(
        Names column_names_,
        const StorageSnapshotPtr & storage_snapshot,
        std::shared_ptr<const Blocks> data_,
        std::shared_ptr<std::atomic<size_t>> parallel_execution_index_,
        InitializerFunc initializer_func_ = {})
        : ISource(storage_snapshot->getSampleBlockForColumns(column_names_), true, ProcessorID::MemorySourceID)
        , column_names_and_types(storage_snapshot->getColumnsByNames(
            GetColumnsOptions(GetColumnsOptions::All).withSubcolumns().withExtendedObjects(), column_names_))
        , data(data_)
        , parallel_execution_index(parallel_execution_index_)
        , initializer_func(std::move(initializer_func_))
    {
    }

    String getName() const override { return "Memory"; }

protected:
    Chunk generate() override
    {
        if (initializer_func)
        {
            initializer_func(data);
            initializer_func = {};
        }

        size_t current_index = getAndIncrementExecutionIndex();

        if (!data || current_index >= data->size())
        {
            return {};
        }

        /// Deep clone the block since if the underlying columns are shared
        /// they could be manipulated by down stream processors which makes
        /// data blocks corrupted
        Block src{(*data)[current_index].deepClone()};

        Columns columns;
        size_t num_columns = column_names_and_types.size();
        columns.reserve(num_columns);

        auto name_and_type = column_names_and_types.begin();
        for (size_t i = 0; i < num_columns; ++i)
        {
            columns.emplace_back(tryGetColumnFromBlock(src, *name_and_type));
            ++name_and_type;
        }

        fillMissingColumns(columns, src.rows(), column_names_and_types, column_names_and_types, {}, /*metadata_snapshot=*/ nullptr);
        assert(std::all_of(columns.begin(), columns.end(), [](const auto & column) { return column != nullptr; }));

        return Chunk(std::move(columns), src.rows());
    }

private:
    size_t getAndIncrementExecutionIndex()
    {
        if (parallel_execution_index)
        {
            return (*parallel_execution_index)++;
        }
        else
        {
            return execution_index++;
        }
    }

    const NamesAndTypesList column_names_and_types;
    size_t execution_index = 0;
    std::shared_ptr<const Blocks> data;
    std::shared_ptr<std::atomic<size_t>> parallel_execution_index;
    InitializerFunc initializer_func;
};


class MemorySink final : public SinkToStorage
{
public:
    MemorySink(
        StorageMemory & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        ContextPtr context)
        : SinkToStorage(metadata_snapshot_->getSampleBlock(), ProcessorID::MemorySinkID)
        , storage(storage_)
        , storage_snapshot(storage_.getStorageSnapshot(metadata_snapshot_, context))
    {
    }

    String getName() const override { return "MemorySink"; }

    void consume(Chunk chunk) override
    {
        auto block = getHeader().cloneWithColumns(chunk.getColumns());
        storage_snapshot->metadata->check(block, true);

        if (storage.getMemorySettingsRef()[MemorySetting::compress])
        {
            Block compressed_block;
            for (const auto & elem : block)
                compressed_block.insert({ elem.column->compress(), elem.type, elem.name });

            new_blocks.emplace_back(compressed_block);
        }
        else
        {
            new_blocks.emplace_back(block);
        }
    }

    void onFinish() override
    {
        size_t inserted_bytes = 0;
        size_t inserted_rows = 0;

        for (const auto & block : new_blocks)
        {
            inserted_bytes += block.allocatedBytes();
            inserted_rows += block.rows();
        }

        std::lock_guard lock(storage.mutex);

        auto new_data = std::make_unique<Blocks>(*(storage.data.get()));
        UInt64 new_total_rows = storage.total_size_rows.load(std::memory_order_relaxed) + inserted_rows;
        UInt64 new_total_bytes = storage.total_size_bytes.load(std::memory_order_relaxed) + inserted_bytes;
        const auto & memory_settings = storage.getMemorySettingsRef();
        while (!new_data->empty()
               && ((memory_settings[MemorySetting::max_bytes_to_keep] && new_total_bytes > memory_settings[MemorySetting::max_bytes_to_keep])
                   || (memory_settings[MemorySetting::max_rows_to_keep] && new_total_rows > memory_settings[MemorySetting::max_rows_to_keep])))
        {
            Block oldest_block = new_data->front();
            UInt64 rows_to_remove = oldest_block.rows();
            UInt64 bytes_to_remove = oldest_block.allocatedBytes();
            if (new_total_bytes - bytes_to_remove < memory_settings[MemorySetting::min_bytes_to_keep]
                || new_total_rows - rows_to_remove < memory_settings[MemorySetting::min_rows_to_keep])
            {
                break; // stop - removing next block will put us under min_bytes / min_rows threshold
            }

            // delete old block from current storage table
            new_total_rows -= rows_to_remove;
            new_total_bytes -= bytes_to_remove;
            new_data->erase(new_data->begin());
        }

        // append new data to modified storage table and commit
        new_data->insert(new_data->end(), new_blocks.begin(), new_blocks.end());

        storage.data.set(std::move(new_data));
        storage.total_size_bytes.fetch_add(inserted_bytes, std::memory_order_relaxed);
        storage.total_size_rows.fetch_add(inserted_rows, std::memory_order_relaxed);
    }

private:
    Blocks new_blocks;

    StorageMemory & storage;
    StorageSnapshotPtr storage_snapshot;
};


StorageMemory::StorageMemory(
    const StorageID & table_id_,
    ColumnsDescription columns_description_,
    ConstraintsDescription constraints_,
    const String & comment,
    const MemorySettings & memory_settings_)
    : IStorage(table_id_)
    , data(std::make_unique<const Blocks>())
    , memory_settings(memory_settings_)
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(std::move(columns_description_));
    storage_metadata.setConstraints(std::move(constraints_)); /// NOLINT(performance-move-const-arg)
    storage_metadata.setComment(comment);
    storage_metadata.setSettingsChanges(memory_settings.getSettingsChangesQuery());
    setInMemoryMetadata(storage_metadata);
}

StorageSnapshotPtr StorageMemory::getStorageSnapshot(const StorageMetadataPtr & metadata_snapshot, ContextPtr /*query_context*/) const
{
    auto snapshot_data = std::make_unique<SnapshotData>();
    snapshot_data->blocks = data.get();

    return std::make_shared<StorageSnapshot>(*this, metadata_snapshot, std::move(snapshot_data));  /// proton: updates. object_columns removed
}

Pipe StorageMemory::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    size_t num_streams)
{
    storage_snapshot->check(column_names);

    const auto & snapshot_data = assert_cast<const SnapshotData &>(*storage_snapshot->data);
    auto current_data = snapshot_data.blocks;

    if (delay_read_for_global_subqueries)
    {
        /// Note: for global subquery we use single source.
        /// Mainly, the reason is that at this point table is empty,
        /// and we don't know the number of blocks are going to be inserted into it.
        ///
        /// It may seem to be not optimal, but actually data from such table is used to fill
        /// set for IN or hash table for JOIN, which can't be done concurrently.
        /// Since no other manipulation with data is done, multiple sources shouldn't give any profit.

        return Pipe(std::make_shared<MemorySource>(
            column_names,
            storage_snapshot,
            nullptr /* data */,
            nullptr /* parallel execution index */,
            [current_data](std::shared_ptr<const Blocks> & data_to_initialize)
            {
                data_to_initialize = current_data;
            }));
    }

    size_t size = current_data->size();

    if (num_streams > size)
        num_streams = size;

    Pipes pipes;

    auto parallel_execution_index = std::make_shared<std::atomic<size_t>>(0);

    for (size_t stream = 0; stream < num_streams; ++stream)
    {
        pipes.emplace_back(std::make_shared<MemorySource>(column_names, storage_snapshot, current_data, parallel_execution_index));
    }

    return Pipe::unitePipes(std::move(pipes));
}


SinkToStoragePtr StorageMemory::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr context)
{
    return std::make_shared<MemorySink>(*this, metadata_snapshot, context);
}


void StorageMemory::drop()
{
    data.set(std::make_unique<Blocks>());
    total_size_bytes.store(0, std::memory_order_relaxed);
    total_size_rows.store(0, std::memory_order_relaxed);
}

static inline void updateBlockData(Block & old_block, const Block & new_block)
{
    for (const auto & it : new_block)
    {
        auto col_name = it.name;
        auto & col_with_type_name = old_block.getByName(col_name);
        col_with_type_name.column = it.column;
    }
}

void StorageMemory::checkMutationIsPossible(const MutationCommands & /*commands*/, const Settings & /*settings*/) const
{
    /// Some validation will be added
}

void StorageMemory::mutate(const MutationCommands & commands, ContextPtr context)
{
    std::lock_guard lock(mutex);
    auto metadata_snapshot = getInMemoryMetadataPtr();
    auto storage = getStorageID();
    auto storage_ptr = DatabaseCatalog::instance().getTable(storage, context);

    /// When max_threads > 1, the order of returning blocks is uncertain,
    /// which will lead to inconsistency after updateBlockData.
    auto new_context = Context::createCopy(context);
    new_context->setSetting("max_streams_to_max_threads_ratio", 1);
    new_context->setSetting("max_threads", 1);

    MutationsInterpreter::Settings settings(true);
    auto interpreter = std::make_unique<MutationsInterpreter>(storage_ptr, metadata_snapshot, commands, new_context, settings);
    auto pipeline = QueryPipelineBuilder::getPipeline(interpreter->execute());
    PullingPipelineExecutor executor(pipeline);

    Blocks out;
    Block block;
    while (executor.pull(block))
    {
        if (memory_settings[MemorySetting::compress])
            for (auto & elem : block)
                elem.column = elem.column->compress();

        out.push_back(block);
    }

    std::unique_ptr<Blocks> new_data;

    // all column affected
    if (interpreter->isAffectingAllColumns())
    {
        new_data = std::make_unique<Blocks>(out);
    }
    else
    {
        /// just some of the column affected, we need update it with new column
        new_data = std::make_unique<Blocks>(*(data.get()));
        auto data_it = new_data->begin();
        auto out_it = out.begin();

        while (data_it != new_data->end())
        {
            /// Mutation does not change the number of blocks
            assert(out_it != out.end());

            updateBlockData(*data_it, *out_it);
            ++data_it;
            ++out_it;
        }

        assert(out_it == out.end());
    }

    size_t rows = 0;
    size_t bytes = 0;
    for (const auto & buffer : *new_data)
    {
        rows += buffer.rows();
        bytes += buffer.bytes();
    }
    total_size_bytes.store(bytes, std::memory_order_relaxed);
    total_size_rows.store(rows, std::memory_order_relaxed);
    data.set(std::move(new_data));
}


void StorageMemory::truncate(
    const ASTPtr &, const StorageMetadataPtr &, ContextPtr, TableExclusiveLockHolder &)
{
    data.set(std::make_unique<Blocks>());
    total_size_bytes.store(0, std::memory_order_relaxed);
    total_size_rows.store(0, std::memory_order_relaxed);
}

void StorageMemory::alter(const DB::AlterCommands & params, DB::ContextPtr context, DB::IStorage::AlterLockHolder & /*alter_lock_holder*/)
{
    auto table_id = getStorageID();
    StorageInMemoryMetadata new_metadata = getInMemoryMetadata();
    params.apply(new_metadata, context);

    if (params.isSettingsAlter())
    {
        auto & settings_changes = new_metadata.settings_changes->as<ASTSetQuery &>();
        auto changed_settings = memory_settings;
        changed_settings.applyChanges(settings_changes.changes);
        changed_settings.sanityCheck();

        /// When modifying the values of max_bytes_to_keep and max_rows_to_keep to be smaller than the old values,
        /// the old data needs to be removed.
        if (!memory_settings[MemorySetting::max_bytes_to_keep] || memory_settings[MemorySetting::max_bytes_to_keep] > changed_settings[MemorySetting::max_bytes_to_keep]
            || !memory_settings[MemorySetting::max_rows_to_keep] || memory_settings[MemorySetting::max_rows_to_keep] > changed_settings[MemorySetting::max_rows_to_keep])
        {
            std::lock_guard lock(mutex);

            auto new_data = std::make_unique<Blocks>(*(data.get()));
            UInt64 new_total_rows = total_size_rows.load(std::memory_order_relaxed);
            UInt64 new_total_bytes = total_size_bytes.load(std::memory_order_relaxed);
            while (!new_data->empty()
                   && ((changed_settings[MemorySetting::max_bytes_to_keep] && new_total_bytes > changed_settings[MemorySetting::max_bytes_to_keep])
                       || (changed_settings[MemorySetting::max_rows_to_keep] && new_total_rows > changed_settings[MemorySetting::max_rows_to_keep])))
            {
                Block oldest_block = new_data->front();
                UInt64 rows_to_remove = oldest_block.rows();
                UInt64 bytes_to_remove = oldest_block.allocatedBytes();
                if (new_total_bytes - bytes_to_remove < changed_settings[MemorySetting::min_bytes_to_keep]
                    || new_total_rows - rows_to_remove < changed_settings[MemorySetting::min_rows_to_keep])
                {
                    break; // stop - removing next block will put us under min_bytes / min_rows threshold
                }

                // delete old block from current storage table
                new_total_rows -= rows_to_remove;
                new_total_bytes -= bytes_to_remove;
                new_data->erase(new_data->begin());
            }

            data.set(std::move(new_data));
            total_size_rows.store(new_total_rows, std::memory_order_relaxed);
            total_size_bytes.store(new_total_bytes, std::memory_order_relaxed);
        }
        memory_settings = std::move(changed_settings);
    }

    DatabaseCatalog::instance().getDatabase(table_id.database_name)->alterTable(context, table_id, new_metadata);
    setInMemoryMetadata(new_metadata);
}

void StorageMemory::checkAlterIsPossible(const AlterCommands & commands, ContextPtr) const
{
    for (const auto & command : commands)
    {
        if (command.type != AlterCommand::Type::ADD_COLUMN && command.type != AlterCommand::Type::MODIFY_COLUMN
            && command.type != AlterCommand::Type::DROP_COLUMN && command.type != AlterCommand::Type::COMMENT_COLUMN
            && command.type != AlterCommand::Type::COMMENT_TABLE && command.type != AlterCommand::Type::RENAME_COLUMN
            && command.type != AlterCommand::Type::MODIFY_SETTING)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Alter of type '{}' is not supported by storage {}",
                command.type, getName());
    }
}

std::optional<UInt64> StorageMemory::totalRows(const Settings &) const
{
    /// All modifications of these counters are done under mutex which automatically guarantees synchronization/consistency
    /// When run concurrently we are fine with any value: "before" or "after"
    return total_size_rows.load(std::memory_order_relaxed);
}

std::optional<UInt64> StorageMemory::totalBytes(const Settings &) const
{
    return total_size_bytes.load(std::memory_order_relaxed);
}

void registerStorageMemory(StorageFactory & factory)
{
    factory.registerStorage("Memory", [](const StorageFactory::Arguments & args)
    {
        if (!args.engine_args.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Engine {} doesn't support any arguments ({} given)",
                args.engine_name, args.engine_args.size());

        bool has_settings = args.storage_def->settings;
        MemorySettings settings;
        if (has_settings)
            settings.loadFromQuery(*args.storage_def);

        settings.sanityCheck();

        return std::make_shared<StorageMemory>(args.table_id, args.columns, args.constraints, args.comment, settings);
    },
    {
        .supports_settings = true,
        .supports_parallel_insert = true,
    });
}

}
