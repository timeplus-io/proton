#include "Sessionizer.h"

#include <Columns/ColumnDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/Chunk.h>
#include <Common/ProtonCommon.h>

namespace DB
{
namespace Streaming
{
Sessionizer::Sessionizer(
    const Block & input_header_, const Block & /*output_header_*/, ExpressionActionsPtr start_actions_, ExpressionActionsPtr end_actions_)
    : input_header(input_header_), start_actions(std::move(start_actions_)), end_actions(std::move(end_actions_))
{
    assert((start_actions && end_actions) || (!start_actions && !end_actions));
}

void Sessionizer::sessionize(Chunk & chunk)
{
    auto rows = chunk.getNumRows();
    if (!rows || !start_actions)
    {
        addPredicateColumns(chunk);
        return;
    }

    size_t insert_pos = 0;

    auto block = input_header.cloneWithColumns(chunk.detachColumns());

    Block start_block;
    auto start_required_columns = start_actions->getRequiredColumns();

    if (start_required_columns.empty())
    {
        /// const expression
        start_block.insert(block.getByPosition(0));
    }
    else
    {
        start_block.reserve(start_required_columns.size());

        if (start_required_pos.empty())
        {
            for (auto & name : start_required_columns)
                start_required_pos.push_back(block.getPositionByName(name));
        }

        for (auto pos : start_required_pos)
            start_block.insert(block.getByPosition(pos));
    }

    start_actions->execute(start_block);
    block.insert(insert_pos++, {start_block.getColumns()[0], std::make_shared<DataTypeBool>(), ProtonConsts::STREAMING_SESSION_START});

    Block end_block;
    auto end_required_columns = end_actions->getRequiredColumns();

    if (end_required_columns.empty())
    {
        /// const expression
        end_block.insert(block.getByPosition(0));
    }
    else
    {
        end_block.reserve(end_required_columns.size());

        if (end_required_pos.empty())
        {
            for (auto & name : end_required_columns)
                end_required_pos.push_back(block.getPositionByName(name));
        }

        for (auto pos : end_required_pos)
            end_block.insert(block.getByPosition(pos));
    }
    end_actions->execute(end_block);
    block.insert(insert_pos++, {end_block.getColumns()[0], std::make_shared<DataTypeBool>(), ProtonConsts::STREAMING_SESSION_END});

    chunk.setColumns(block.getColumns(), block.rows());
}

void Sessionizer::addPredicateColumns(Chunk & chunk)
{
    auto rows = chunk.getNumRows();
    auto data_type = std::make_shared<DataTypeBool>();

    auto col_start = data_type->createColumnConst(rows, true);
    auto col_end = data_type->createColumnConst(rows, true);

    size_t insert_pos = 0;
    /// chunk.addColumn(insert_pos++, col_start->convertToFullColumnIfConst());
    /// chunk.addColumn(insert_pos++, col_end->convertToFullColumnIfConst());

    chunk.addColumn(insert_pos++, std::move(col_start));
    chunk.addColumn(insert_pos++, std::move(col_end));
}

}
}
