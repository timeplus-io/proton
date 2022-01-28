#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/castColumn.h>

#include <base/map.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ARGUMENT_OUT_OF_BOUND;
}

namespace
{
    static constexpr auto * STREAMING_NEIGHBOR = "neighbor";

    /// Cache prev-columns + current-column
    class ColumnsCache final
    {
    private:
        using ColumnsWithIndex = std::deque<std::pair<size_t, ColumnPtr>>;
        Int64 max_prev_cache_rows = 0;
        ColumnsWithIndex columns;
        Int64 curr_cache_rows = 0;
        mutable std::mutex mutex;

    public:
        ColumnsCache(size_t max_prev_cache_rows_) : max_prev_cache_rows(max_prev_cache_rows_) { }

        void add(ColumnPtr column)
        {
            std::lock_guard lock(mutex);
            /// If prev columns cache full, we shall remove overflowing and useless column and/or set a valid begin_cursor
            Int64 overflow_size = curr_cache_rows - max_prev_cache_rows;
            while (overflow_size > 0)
            {
                auto & front_column = columns.front();
                if (front_column.second->size() > size_t(overflow_size))
                {
                    /// For example offset = -1 (max_prev_cache_rows = 1), but there is one cached prev block that has 3 rows => `overflow_size = 2`
                    /// the actual cached prev rows begin with index 2 (I.e. <2, column> )
                    front_column.first = overflow_size;
                    break;
                }

                curr_cache_rows -= front_column.second->size();
                columns.pop_front();
                overflow_size = curr_cache_rows - max_prev_cache_rows;
            }

            /// Add current column
            columns.push_back({0, column});
            curr_cache_rows += column->size();
        }

        std::pair<Int64, ColumnsWithIndex> getRowsAndColumnsWithIndex() const
        {
            std::lock_guard lock(mutex);
            return {curr_cache_rows, columns};
        }
    };

    DataTypePtr checkAndGetReturnType(const DataTypes & arguments)
    {
        size_t number_of_arguments = arguments.size();

        if (number_of_arguments < 2 || number_of_arguments > 3)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function '{}' doesn't match: passed {}, should be from 2 or 3",
                STREAMING_NEIGHBOR,
                number_of_arguments);

        // second argument must be an integer
        if (!isInteger(arguments[1]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of second argument of function '{}' - should be an integer",
                arguments[1]->getName(),
                STREAMING_NEIGHBOR);
        else if (arguments[1]->isNullable())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type  {} of second argument of function '{}' - can not be Nullable",
                arguments[1]->getName(),
                STREAMING_NEIGHBOR);

        // check that default value column has supertype with first argument
        if (number_of_arguments == 3)
            return getLeastSupertype({arguments[0], arguments[2]});

        return arguments[0];
    }

    /// Specified streaming query, Limits:
    /// 1) The offset argument must be constant non-positive number (i.e. only reach current or prev rows)
    /// Example:
    /// SELECT c1, neighborOnTail(c1, -1) as c2:
    /// (Input)             (Output)
    ///  <<<                    >>>
    /// | c1 |              | c1 | c2 |
    /// | 10 |              | 10 | 0  |
    /// | 20 |              | 20 | 10 |
    ///
    ///  <<<                    >>>
    /// | c1 |              | c1 | c2 |
    /// | 30 |              | 30 | 20 |
    /// | 40 |              | 40 | 30 |

    class FunctionStreamingNeighbor : public IFunction
    {
    private:
        UInt32 prev_offset = 0;
        mutable ColumnsCache prev_offset_columns;

    public:
        explicit FunctionStreamingNeighbor(Int64 offset_) : prev_offset(std::abs(offset_)), prev_offset_columns(prev_offset)
        {
            /// Protection from possible overflow.
            if (offset_ >= 0 || offset_ < -(1 << 30))
                throw Exception(
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                    "Invalid offset: {} in function {}, expected [{}, -1]",
                    offset_,
                    getName(),
                    -(1 << 30));
        }

        /// Get the name of the function.
        String getName() const override { return STREAMING_NEIGHBOR; }

        size_t getNumberOfArguments() const override { return 0; }

        bool isVariadic() const override { return true; }

        bool isStateful() const override { return true; }

        bool isDeterministic() const override { return false; }

        bool isDeterministicInScopeOfQuery() const override { return false; }

        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

        bool useDefaultImplementationForNulls() const override { return false; }

        bool useDefaultImplementationForConstants() const override { return false; }

        /// We do not use default implementation for LowCardinality because this is not a pure function.
        /// If used, optimization for LC may execute function only for dictionary, which gives wrong result.
        bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

        DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override { return checkAndGetReturnType(arguments); }

        ColumnPtr
        executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
        {
            const ColumnWithTypeAndName & source_elem = arguments[0];
            ColumnPtr source_column_casted = castColumn(source_elem, result_type);

            /// Degenerate case, just copy source column as is.
            if (input_rows_count == 0)
                return source_column_casted;

            prev_offset_columns.add(source_column_casted);

            auto result_column = result_type->createColumn();
            Int64 missing_rows = input_rows_count;
            auto insert_range_from = [&](bool is_const, const ColumnPtr & src, Int64 begin, Int64 size) {
                /// Saturation of bounds.
                if (size > missing_rows)
                    size = missing_rows;

                missing_rows -= size;

                if (!src)
                {
                    for (Int64 i = 0; i < size; ++i)
                        result_column->insertDefault();
                }
                else if (is_const)
                {
                    for (Int64 i = 0; i < size; ++i)
                        result_column->insertFrom(*src, 0);
                }
                else
                {
                    result_column->insertRangeFrom(*src, begin, size);
                }
            };

            auto [total_rows, columns_with_index] = prev_offset_columns.getRowsAndColumnsWithIndex();

            /// insert default
            Int64 default_rows = prev_offset + input_rows_count - total_rows;
            if (default_rows > 0)
            {
                ColumnPtr default_column_casted;
                bool default_is_constant = false;
                bool has_defaults = arguments.size() == 3;

                if (has_defaults)
                {
                    default_column_casted = castColumn(arguments[2], result_type);
                    default_is_constant = isColumnConst(*default_column_casted);
                }

                if (default_is_constant)
                    default_column_casted = assert_cast<const ColumnConst &>(*default_column_casted).getDataColumnPtr();

                insert_range_from(default_is_constant, default_column_casted, 0, default_rows);
            }

            /// insert prev values
            for (auto [begin, column] : columns_with_index)
            {
                if (missing_rows <= 0)
                    break;

                bool is_constant = isColumnConst(*column);
                if (is_constant)
                    column = assert_cast<const ColumnConst &>(*column).getDataColumnPtr();

                insert_range_from(is_constant, column, begin, column->size() - begin);
            }

            return result_column;
        }
    };

    class NeighborOverloadResolver : public IFunctionOverloadResolver
    {
    public:
        static constexpr auto * name = "__streaming_neighbor";

        static FunctionOverloadResolverPtr create(ContextPtr context)
        {
            return std::make_unique<NeighborOverloadResolver>(context);
        }

        explicit NeighborOverloadResolver(ContextPtr context_) : context(context_) { }

        String getName() const override { return name; }
        size_t getNumberOfArguments() const override { return 0; }
        bool isVariadic() const override { return true; }

        FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
        {
            /// Check ahead in `getReturnTypeImpl`
            /// neighbor(column, offset[, default_value])
            assert(arguments.size() >= 2);
            const auto * offset_col = checkAndGetColumn<ColumnConst>(arguments[1].column.get());
            if (!offset_col)
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Invalid offset, only support constant offset in current query. '{}'", getName());

            return std::make_unique<FunctionToFunctionBaseAdaptor>(
                std::make_shared<FunctionStreamingNeighbor>(offset_col->getInt(0)),
                collections::map<DataTypes>(arguments, [](const auto & elem) { return elem.type; }),
                return_type);
        }

        DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override { return checkAndGetReturnType(arguments); }

    private:
        ContextPtr context;
    };
}

void registerFunctionStreamingNeighbor(FunctionFactory & factory)
{
    factory.registerFunction<NeighborOverloadResolver>(FunctionFactory::CaseSensitive);
}

}
