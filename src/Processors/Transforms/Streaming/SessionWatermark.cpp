#include "SessionWatermark.h"

#include <Common/ProtonCommon.h>
#include "Watermark.h"

#include <Columns/ColumnDecimal.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <base/logger_useful.h>
#include <Common/StringUtils/StringUtils.h>

namespace DB
{
namespace Streaming
{
SessionWatermark::SessionWatermark(
    WatermarkSettings && watermark_settings_,
    bool proc_time_,
    ExpressionActionsPtr start_actions_,
    ExpressionActionsPtr end_actions_,
    Poco::Logger * log_)
    : HopTumbleBaseWatermark(std::move(watermark_settings_), proc_time_, log_), start_actions(start_actions_), end_actions(end_actions_)
{
}

void SessionWatermark::doProcess(Block & block)
{
    auto rows = block.rows();
    size_t insert_pos = 0;
    assert((start_actions && end_actions) || (!start_actions && !end_actions));

    if (start_actions && end_actions)
    {
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
    }
    else
    {
        auto start_data_type = std::make_shared<DataTypeBool>();
        MutableColumnPtr col_start = start_data_type->createColumn();
        col_start->reserve(rows);

        for (size_t i = 0; i < rows; i++)
            col_start->insert(true);

        block.insert(insert_pos++, {std::move(col_start), std::make_shared<DataTypeBool>(), ProtonConsts::STREAMING_SESSION_START});

        auto end_data_type = std::make_shared<DataTypeBool>();
        MutableColumnPtr col_end = end_data_type->createColumn();
        col_end->reserve(rows);

        for (size_t i = 0; i < rows; i++)
            col_end->insert(true);

        block.insert(insert_pos++, {std::move(col_end), std::make_shared<DataTypeBool>(), ProtonConsts::STREAMING_SESSION_END});
    }
}

void SessionWatermark::handleIdlenessWatermark(Block & block)
{
    /// insert '__tp_session_id' column
    auto rows = block.rows();
    {
        size_t insert_pos = 0;
        auto begin_data_type = std::make_shared<DataTypeBool>();
        MutableColumnPtr col_begin = begin_data_type->createColumn();
        col_begin->reserve(rows);
        block.insert(insert_pos++, {begin_data_type, ProtonConsts::STREAMING_SESSION_START});

        auto end_data_type = std::make_shared<DataTypeBool>();
        MutableColumnPtr col_end = end_data_type->createColumn();
        col_end->reserve(rows);
        block.insert(insert_pos++, {end_data_type, ProtonConsts::STREAMING_SESSION_END});
    }
    /// TODO: add watermark
}

}
}
