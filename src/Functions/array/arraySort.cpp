#include <Functions/array/arraySort.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

template <bool positive>
struct Less
{
    const IColumn & column;

    explicit Less(const IColumn & column_) : column(column_) { }

    bool operator()(size_t lhs, size_t rhs) const
    {
        if constexpr (positive)
            return column.compareAt(lhs, rhs, column, 1) < 0;
        else
            return column.compareAt(lhs, rhs, column, -1) > 0;
    }
};

}

template <bool positive, bool is_partial>
ColumnPtr ArraySortImpl<positive, is_partial>::execute(
    const ColumnArray & array,
    ColumnPtr mapped,
    const ColumnWithTypeAndName * fixed_arguments)
{
    [[maybe_unused]] const auto limit = [&]() -> size_t
    {
        if constexpr (is_partial)
        {
            if (!fixed_arguments)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Expected fixed arguments to get the limit for partial array sort"
                );
            return fixed_arguments[0].column.get()->getUInt(0);
        }
        return 0;
    }();

    const ColumnArray::Offsets & offsets = array.getOffsets();

    size_t size = offsets.size();
    size_t nested_size = array.getData().size();
    IColumn::Permutation permutation(nested_size);

    for (size_t i = 0; i < nested_size; ++i)
        permutation[i] = i;

    ColumnArray::Offset current_offset = 0;
    for (size_t i = 0; i < size; ++i)
    {
        auto next_offset = offsets[i];
        if constexpr (is_partial)
        {
            if (limit)
            {
                const auto effective_limit = std::min<size_t>(limit, next_offset - current_offset);
                ::partial_sort(&permutation[current_offset], &permutation[current_offset + effective_limit], &permutation[next_offset], Less<positive>(*mapped));
            }
            else
                ::sort(&permutation[current_offset], &permutation[next_offset], Less<positive>(*mapped));
        }
        else
            ::sort(&permutation[current_offset], &permutation[next_offset], Less<positive>(*mapped));
        current_offset = next_offset;
    }

    return ColumnArray::create(array.getData().permute(permutation, 0), array.getOffsetsPtr());
}

REGISTER_FUNCTION(ArraySort)
{
    factory.registerFunction<FunctionArraySort>();
    factory.registerFunction<FunctionArrayReverseSort>();
    factory.registerFunction<FunctionArrayPartialSort>();
    factory.registerFunction<FunctionArrayPartialReverseSort>();
}

}
