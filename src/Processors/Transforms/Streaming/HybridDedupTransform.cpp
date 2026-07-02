#include <Processors/Transforms/Streaming/HybridDedupTransform.h>

#include <Checkpoint/CheckpointCoordinator.h>
#include <Columns/ColumnConst.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Streaming/TableFunctionDescription.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Common/SipHash.h>

namespace DB::Streaming
{
HybridDedupTransform::HybridDedupTransform(
    const Block & input_header,
    const Block & output_header,
    TableFunctionDescriptionPtr dedup_func_desc_,
    const String & spill_dir,
    const String & kv_options)
    : ISimpleTransform(input_header, output_header, false, ProcessorID::HybridDedupTransformID)
    , dedup_func_desc(std::move(dedup_func_desc_))
    , chunk_header(output_header.getColumns(), 0)
    , logger(getLogger("HybridDedupTransform"))
{
    chassert(dedup_func_desc);

    init(input_header, output_header, spill_dir, kv_options);
}

void HybridDedupTransform::transform(Chunk & chunk)
{
    if (!chunk.hasRows())
    {
        /// The downstream header is different than the output of this transform
        /// We need use the current output header
        chunk.setColumns(chunk_header.cloneEmptyColumns(), 0);
        return;
    }

    auto input_block = getInputPort().getHeader().cloneWithColumns(chunk.detachColumns());

    Block transformed_block;

    /// Most of the time, we copied only one column
    for (auto pos : expr_column_positions)
        transformed_block.insert(input_block.getByPosition(pos));

    dedup_func_desc->expr_before_table_function->execute(transformed_block);

    chassert(transformed_block);

    auto filtered_rows = transformed_block.rows();
    auto filter = populateKeySetsAndCalculateResults(transformed_block.getColumnsWithTypeAndName(), filtered_rows);

    for (auto output_pos : output_column_positions)
    {
        if (output_pos < 0)
        {
            auto & col_with_name_type = transformed_block.getByPosition(-1 - output_pos);
            chunk.addColumn(col_with_name_type.column->filter(filter, filtered_rows));
        }
        else
        {
            auto & col_with_name_type = input_block.getByPosition(output_pos);
            chunk.addColumn(col_with_name_type.column->filter(filter, filtered_rows));
        }
    }
}

void HybridDedupTransform::init(
    const Block & input_header, const Block & output_header, const String & spill_dir, const String & kv_options)
{
    /// Calculate the positions of dependent columns in input chunk
    expr_column_positions.reserve(dedup_func_desc->input_columns.size());
    for (const auto & col_name : dedup_func_desc->input_columns)
        expr_column_positions.push_back(input_header.getPositionByName(col_name));

    auto transformed_header = input_header.cloneEmpty();
    dedup_func_desc->expr_before_table_function->execute(transformed_header);

    /// Calculate the positions of output columns in input chunk
    output_column_positions.reserve(output_header.columns());
    for (const auto & col_with_type : output_header)
    {
        if (transformed_header.has(col_with_type.name))
            /// we use negative pos `-1, ... , -n` to indicate transformed columns pos (0, ..., n-1)
            output_column_positions.push_back(-1 - transformed_header.getPositionByName(col_with_type.name));
        else
            output_column_positions.push_back(input_header.getPositionByName(col_with_type.name));
    }

    initKeySet(spill_dir, kv_options);
}

void HybridDedupTransform::initKeySet(const String & spill_dir, const String & kv_options)
{
    UInt64 limit = 10000;
    Int64 limit_sec = -1;

    /// dedup(column1, column2, ..., [timeout, [limit]])

    const auto & args = dedup_func_desc->func_ast->as<ASTFunction>()->arguments->children;
    chassert(!args.empty());

    auto check_timeout = [&](size_t pos) {
        /// Check if last argument is interval literal
        if (const auto * func = args[pos]->as<ASTFunction>(); func)
        {
            if (func->name == "to_interval_second" && args.size() >= 2)
            {
                if (auto * lit = func->arguments->children.front()->as<ASTLiteral>(); lit)
                    limit_sec = lit->value.get<Int64>();
            }
        }
    };

    auto check_limit = [&](size_t pos) {
        if (auto * lit = args[pos]->as<ASTLiteral>(); lit)
        {
            if (isInt64OrUInt64FieldType(lit->value.getType()) && args.size() >= 3)
            {
                limit = lit->value.get<UInt64>();
                return true;
            }
        }
        return false;
    };

    if (args.size() > 2)
    {
        if (check_limit(args.size() - 1))
            check_timeout(args.size() - 2);
        else
            check_timeout(args.size() - 1);
    }
    else if (args.size() == 2)
    {
        check_timeout(args.size() - 1);
    }

    HybridHashTableConfig config;
    config.base_conf.spill_dir_path = spill_dir;
    config.base_conf.max_hot_key_count = limit;
    config.base_conf.ttl = static_cast<Int32>(limit_sec);
    config.base_conf.kv_options = kv_options;
    config.installNoOpCallbacks();

    auto key_serializer = [](const UInt128 & key, WriteBuffer & wb) {
        writeIntBinary(key, wb);
        return ErrorCodes::OK;
    };

    auto key_deserializer = [](UInt128 & key, ReadBuffer & rb) {
        readIntBinary(key, rb);
        return ErrorCodes::OK;
    };

    key_set = std::make_unique<Set>(std::move(config), std::move(key_serializer), std::move(key_deserializer), logger);
}

/// Calculate the hash value of the columns, do filtering if the columns (their hash value) have been seen before
/// otherwise update the hash set with the new hash keys. Prune `expired` hash keys if max key limit or timeout limit whichever
/// reaches
IColumn::Filter HybridDedupTransform::populateKeySetsAndCalculateResults(const ColumnsWithTypeAndName & arguments, size_t & rows)
{
    size_t nargs = arguments.size();

    std::vector<UInt128> hash_keys;
    hash_keys.reserve(rows);

    for (size_t i = 0; i < rows; ++i)
    {
        UInt128 key;
        SipHash hash;

        /// Process row i
        for (size_t j = 0; j < nargs; ++j)
        {
            StringRef value = arguments[j].column->getDataAt(i);
            hash.update(value.data, value.size);
        }

        hash.get128(key);
        hash_keys.push_back(key);
    }

    IColumn::Filter filter;
    filter.reserve(rows);

    auto emplace_results = key_set->emplaceKeys(hash_keys);
    if (emplace_results.hasError())
        throw Exception::createRuntime(emplace_results.errcode, emplace_results.errorString());

    rows = 0; /// Count rows of after filtered
    for (const auto & result : emplace_results.results)
    {
        if (result.isInserted())
        {
            filter.push_back(1);
            ++rows;
        }
        else
        {
            filter.push_back(0);
        }
    }

    return filter;
}

void HybridDedupTransform::doCheckpoint(CheckpointContextPtr ckpt_ctx)
{
    ckpt_ctx->coordinator->checkpoint(getVersion(), getLogicID(), ckpt_ctx, [this](WriteBuffer & wb) { key_set->write(wb); });
}

void HybridDedupTransform::doRecover(CheckpointContextPtr ckpt_ctx)
{
    ckpt_ctx->coordinator->recover(getLogicID(), ckpt_ctx, [this](VersionType /*version*/, ReadBuffer & rb) { key_set->read(rb); });
}

}
