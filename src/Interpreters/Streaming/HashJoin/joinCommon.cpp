#include <Interpreters/Streaming/HashJoin/joinCommon.h>
#include <Interpreters/castColumn.h>

namespace DB::Streaming
{

ColumnWithTypeAndName copyLeftKeyColumnToRight(
    const DataTypePtr & right_key_type,
    const String & renamed_right_column,
    const ColumnWithTypeAndName & left_column,
    const IColumn::Filter * null_map_filter)
{
    ColumnWithTypeAndName right_column = left_column;
    right_column.name = renamed_right_column;

    if (null_map_filter)
        right_column.column = JoinCommon::filterWithBlanks(right_column.column, *null_map_filter);

    bool should_be_nullable = isNullableOrLowCardinalityNullable(right_key_type);
    if (null_map_filter)
        JoinCommon::correctNullabilityInplace(right_column, should_be_nullable, *null_map_filter);
    else
        JoinCommon::correctNullabilityInplace(right_column, should_be_nullable);

    if (!right_column.type->equals(*right_key_type))
    {
        right_column.column = castColumnAccurate(right_column, right_key_type);
        right_column.type = right_key_type;
    }

    right_column.column = right_column.column->convertToFullColumnIfConst();
    return right_column;
}

}
