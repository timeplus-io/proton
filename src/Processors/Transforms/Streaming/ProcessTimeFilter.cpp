#include "ProcessTimeFilter.h"

#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDateTime.h>
#include <Common/timeScale.h>

namespace DB
{

namespace
{
    using ColumnDateTime64 = ColumnDecimal<DateTime64>;
    using ColumnDateTime32 = ColumnVector<UInt32>;

    template <typename TargetColumnType>
    ALWAYS_INLINE UInt64 filter(Columns & columns, const ColumnPtr & time_col, Int64 interval_ago)
    {
        const typename TargetColumnType::Container & time_vec = checkAndGetColumn<TargetColumnType>(time_col.get())->getData();

        auto rows = time_vec.size();
        IColumn::Filter filt(rows, 1);

        UInt64 filtered = 0;
        for (size_t i = 0; i < rows; ++i)
        {
            if (time_vec[i] < interval_ago)
            {
                filt[i] = 0;
                filtered += 1;
            }
        }

        if (filtered > 0)
        {
            for (auto & column: columns)
            {
                column = column->filter(filt, rows - filtered);
            }
        }

        return filtered;
    }
}

namespace ErrorCodes
{
    extern const int UNKNOWN_IDENTIFIER;
    extern const int TYPE_MISMATCH;
}

ProcessTimeFilter::ProcessTimeFilter(const String & column_name_, Int64 interval_seconds_, const Block & header)
    : ISimpleTransform(header, header, false), column_name(column_name_), interval_seconds(interval_seconds_)
{
    const auto * column_with_type = header.findByName(column_name);
    if (!column_with_type)
        throw Exception("Missing target column for process time filtering", ErrorCodes::UNKNOWN_IDENTIFIER);

    timestamp_col_data_type = column_with_type->type;
    timestamp_col_pos = header.getPositionByName(column_name);

    if (isDateTime64(column_with_type->type))
    {
        const auto * type = typeid_cast<const DataTypeDateTime64 *>(column_with_type->type.get());
        assert(type);

        scale = type->getScale();
        is_datetime64 = true;

        multiplier = intExp10(scale);
    }
    else if (isDateTime(column_with_type->type))
    {
        const auto * type = typeid_cast<const DataTypeDateTime *>(column_with_type->type.get());
        assert(type);
        (void)type;
    }
    else
        throw Exception("Target column for process time filtering is not Datetime nor DateTime64", ErrorCodes::TYPE_MISMATCH);
}

void ProcessTimeFilter::transform(Chunk & chunk)
{
    auto rows = chunk.getNumRows();
    auto source_columns = chunk.detachColumns();
    auto source_column = source_columns[timestamp_col_pos];

    UInt64 filtered_rows = 0;
    if (is_datetime64)
    {
        /// Call now64(scale, timezone)
        auto now_column = timestamp_col_data_type->createColumnConst(1, nowSubsecond(scale));
        filtered_rows = filter<ColumnDateTime64>(source_columns, source_column, now_column->get64(0) - interval_seconds * multiplier);
    }
    else
    {
        /// Call now(timezone)
        auto now_column = timestamp_col_data_type->createColumnConst(1, static_cast<UInt64>(time(nullptr)));
        filtered_rows = filter<ColumnDateTime32>(source_columns, source_column, now_column->get64(0) - interval_seconds);
    }

    chunk.setColumns(std::move(source_columns), rows - filtered_rows);
}
}
