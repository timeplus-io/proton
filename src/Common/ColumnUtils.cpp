#include <Common/ColumnUtils.h>

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnVector.h>

namespace DB
{
std::pair<Int64, Int64> columnMinMaxTimestamp(const ColumnPtr & column, const DataTypePtr & type)
{
    auto type_idx = type->getTypeId();
    if (type_idx == TypeIndex::DateTime64)
    {
        auto * col = typeid_cast<DB::ColumnDecimal<DB::DateTime64> *>(column->assumeMutable().get());
        assert(col);

        const auto & timestamps{col->getData()};
        auto result{std::minmax_element(timestamps.begin(), timestamps.end())};
        return {result.first->value, result.second->value};
    }
    else if (type_idx == TypeIndex::DateTime)
    {
        auto * col = typeid_cast<DB::ColumnVector<UInt32> *>(column->assumeMutable().get());
        assert(col);

        const auto & timestamps{col->getData()};
        auto result{std::minmax_element(timestamps.begin(), timestamps.end())};
        return {*result.first, *result.second};
    }

    return {0, 0};
}
}
