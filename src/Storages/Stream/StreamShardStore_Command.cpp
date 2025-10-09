#include <Parsers/ASTOptimizeQuery.h>
#include <Parsers/ParserOptimizeQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/Stream/StorageStream.h>
#include <Storages/Stream/StreamShardStore.h>

namespace DB
{

void StreamShardStore::handleControlCommand(cluster::protocol::OpCode opcode, const Block & control_block, Int64 sn)
{
    switch (opcode)
    {
        case cluster::protocol::OpCode::Truncate:
        {
            auto truncate_command = control_block.getByPosition(2).column->getDataAt(0).toView();
            truncate(truncate_command);
            LOG_INFO(logger, "Done with executing {{{}}} at sn={}", truncate_command, sn);
            break;
        }
        case cluster::protocol::OpCode::AlterPartition:
        {
            auto alter_partition_command = control_block.getByPosition(2).column->getDataAt(0).toView();
            alterPartition(alter_partition_command);
            LOG_INFO(logger, "Done with executing {{{}}} at sn={}", alter_partition_command, sn);
            break;
        }
        case cluster::protocol::OpCode::DeleteRows:
        {
            auto delete_command = control_block.getByPosition(2).column->getDataAt(0).toView();
            deleteRows(delete_command);
            LOG_INFO(logger, "Done with executing {{{}}} at sn={}", delete_command, sn);
            break;
        }
        case cluster::protocol::OpCode::RebuildIndex:
        {
            auto rebuild_index_command = control_block.getByPosition(2).column->getDataAt(0).toView();
            rebuildIndex(rebuild_index_command);
            LOG_INFO(logger, "Done with executing {{{}}} at sn={}", rebuild_index_command, sn);
            break;
        }
        case cluster::protocol::OpCode::DropIndex:
        {
            auto drop_index_command = control_block.getByPosition(2).column->getDataAt(0).toView();
            dropIndex(drop_index_command);
            LOG_INFO(logger, "Done with executing {{{}}} at sn={}", drop_index_command, sn);
            break;
        }
        case cluster::protocol::OpCode::Optimize:
        {
            optimize(sn, control_block);
            break;
        }
        default:
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} is not supported by Stream", magic_enum::enum_name(opcode));
        }
    }
}

void StreamShardStore::truncate(std::string_view /*truncate_partition_command*/)
{
    chassert(storage);
    {
        auto metadata_snapshot = storage_stream.getInMemoryMetadataPtr();
        TableExclusiveLockHolder table_lock;
        ASTPtr ast_;
        storage->truncate(ast_, metadata_snapshot, storage_stream.getContext(), table_lock);
    }
}

void StreamShardStore::alterPartition(std::string_view alter_partition_command)
{
    chassert(storage);
    if (auto command = PartitionCommand::stringToPartitionCommand(alter_partition_command); command)
    {
        auto metadata_snapshot = storage_stream.getInMemoryMetadataPtr();
        storage->alterPartition(metadata_snapshot, PartitionCommands{*command}, storage_stream.getContext());
    }
}

void StreamShardStore::deleteRows(std::string_view update_rows_command)
{
    chassert(storage);
    if (auto commands = MutationCommands::fromAlterCommandString(update_rows_command); commands)
    {
        auto context_ = Context::createCopy(storage_stream.getContext());
        context_->setSetting("mutations_sync", 2);
        storage->mutate(*commands, context_);
    }
}

void StreamShardStore::rebuildIndex(std::string_view rebuild_index_command)
{
    chassert(storage);
    if (auto commands = MutationCommands::fromAlterCommandString(rebuild_index_command); commands)
    {
        auto context_ = Context::createCopy(storage_stream.getContext());
        context_->setSetting("mutations_sync", 2);
        storage->mutate(*commands, context_);
    }
}

void StreamShardStore::dropIndex(std::string_view drop_index_command)
{
    chassert(storage);
    if (auto commands = MutationCommands::fromAlterCommandString(drop_index_command); commands)
    {
        auto context_ = Context::createCopy(storage_stream.getContext());
        context_->setSetting("mutations_sync", 2);
        storage->mutate(*commands, context_);
    }
}

void StreamShardStore::optimize(Int64 sn, const Block & control_block)
{
    /// Get optimize query as AST
    ASTPtr optimize_query;
    if (const auto * string_column = typeid_cast<const ColumnString *>(control_block.getByPosition(2).column.get()))
    {
        String query_str = string_column->getDataAt(0).toString();
        if (!query_str.empty())
        {
            ParserOptimizeQuery parser;
            optimize_query = parseQuery(
                parser, query_str.data(), query_str.data() + query_str.size(), "optimize query", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
        }
    }

    auto final = control_block.getByPosition(3).column->getUInt(0);
    auto deduplicate = control_block.getByPosition(4).column->getUInt(0);

    /// Handle array column for deduplicate_columns
    Names deduplicate_columns;
    if (deduplicate)
    {
        const auto & array_column = typeid_cast<const ColumnArray &>(*control_block.getByPosition(5).column);
        const auto & string_column = typeid_cast<const ColumnString &>(array_column.getData());
        const auto & offsets = array_column.getOffsets();

        if (!offsets.empty())
        {
            size_t start = 0;
            for (size_t i = 0; i < offsets.size(); ++i)
            {
                size_t end = offsets[i];
                for (size_t j = start; j < end; ++j)
                    deduplicate_columns.push_back(string_column.getDataAt(j).toString());
                start = end;
            }
        }
    }

    LOG_INFO(logger, "Start optimizing stream for query={} at sn={}", optimize_query ? queryToString(optimize_query) : "", sn);

    Stopwatch stopwatch;
    SCOPE_EXIT({
        LOG_INFO(
            logger,
            "Finish optimizing stream for query={} at sn={}, took={}ms",
            optimize_query ? queryToString(optimize_query) : "",
            sn,
            stopwatch.elapsedMilliseconds());
    });

    /// Do full manual compaction if we have a storage
    if (storage)
    {
        storage->optimize(
            optimize_query,
            storage->getInMemoryMetadataPtr(),
            optimize_query ? optimize_query->as<ASTOptimizeQuery>()->partition : nullptr,
            final,
            deduplicate,
            deduplicate_columns,
            storage_stream.getContext());
    }

    LOG_INFO(logger, "Done with executing optimize for query={} at sn={}", optimize_query ? queryToString(optimize_query, true) : "", sn);
}
}
