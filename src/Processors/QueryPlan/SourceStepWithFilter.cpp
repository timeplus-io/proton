#include <Processors/QueryPlan/SourceStepWithFilter.h>

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/ActionsDAG.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
}

Block SourceStepWithFilter::applyPrewhereActions(Block block, const PrewhereInfoPtr & prewhere_info)
{
    if (prewhere_info)
    {
        if (prewhere_info->row_level_filter)
        {
            block = prewhere_info->row_level_filter->updateHeader(block);
            auto & row_level_column = block.getByName(prewhere_info->row_level_column_name);
            if (!row_level_column.type->canBeUsedInBooleanContext())
            {
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER,
                    "Invalid type for filter in PREWHERE: {}",
                    row_level_column.type->getName());
            }

            block.erase(prewhere_info->row_level_column_name);
        }

        {
            block = prewhere_info->prewhere_actions->updateHeader(block);

            auto & prewhere_column = block.getByName(prewhere_info->prewhere_column_name);
            if (!prewhere_column.type->canBeUsedInBooleanContext())
            {
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER,
                    "Invalid type for filter in PREWHERE: {}",
                    prewhere_column.type->getName());
            }

            if (prewhere_info->remove_prewhere_column)
            {
                block.erase(prewhere_info->prewhere_column_name);
            }
            else if (prewhere_info->need_filter)
            {
                if (const auto * type = typeid_cast<const DataTypeNullable *>(prewhere_column.type.get()); type && type->onlyNull())
                {
                    prewhere_column.column = prewhere_column.type->createColumnConst(block.rows(), Null());
                }
                else
                {
                    WhichDataType which(removeNullable(recursiveRemoveLowCardinality(prewhere_column.type)));

                    if (which.isNativeInt() || which.isNativeUInt())
                        prewhere_column.column = prewhere_column.type->createColumnConst(block.rows(), 1u)->convertToFullColumnIfConst();
                    else if (which.isFloat())
                        prewhere_column.column = prewhere_column.type->createColumnConst(block.rows(), 1.0f)->convertToFullColumnIfConst();
                    else
                        throw Exception(
                            ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER,
                            "Illegal type {} of column for filter",
                            prewhere_column.type->getName());
                }
            }
        }
    }

    return block;
}

}
