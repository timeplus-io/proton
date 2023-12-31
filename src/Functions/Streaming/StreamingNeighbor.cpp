#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/castColumn.h>
#include <Common/AllocatorWithMemoryTracking.h>

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
    struct NameToStreamingNeighbor { static constexpr auto * name = "neighbor"; };
    struct NameToLag { static constexpr auto * name = "lag"; };
    struct NameToLags { static constexpr auto * name = "lags"; };

    ALWAYS_INLINE void
    writeColumn(const DB::ColumnPtr & column, const DataTypePtr & column_type, DB::WriteBuffer & wb)
    {
        DB::writeIntBinary(column->size(), wb);

        auto info = column_type->getSerializationInfo(*column);
        auto serialization = column_type->getSerialization(*info);
        bool has_custom = info->hasCustomSerialization();
        writeIntBinary(static_cast<uint8_t>(has_custom), wb);
        if (has_custom)
            info->serialializeKindBinary(wb);

        /// If there are columns-constants - then we materialize them.
        /// (Since the data type does not know how to serialize / deserialize constants.)
        DB::ColumnPtr full_column = column->convertToFullColumnIfConst();
        DB::ISerialization::SerializeBinaryBulkSettings settings;
        settings.getter = [&wb](DB::ISerialization::SubstreamPath) -> DB::WriteBuffer * { return &wb; };
        settings.position_independent_encoding = false;
        settings.low_cardinality_max_dictionary_size = 0; //-V1048

        uint64_t offset = 0, limit = 0;
        DB::ISerialization::SerializeBinaryBulkStatePtr state;
        serialization->serializeBinaryBulkStatePrefix(*full_column, settings, state);
        serialization->serializeBinaryBulkWithMultipleStreams(*full_column, offset, limit, settings, state);
        serialization->serializeBinaryBulkStateSuffix(settings, state);
    }

    ALWAYS_INLINE void readColumn(DB::ColumnPtr & column, const DataTypePtr & column_type, DB::ReadBuffer & rb)
    {
        size_t rows;
        DB::readIntBinary(rows, rb);

        auto info = column_type->createSerializationInfo({});

        uint8_t has_custom;
        DB::readIntBinary(has_custom, rb);
        if (has_custom)
            info->deserializeFromKindsBinary(rb);

        auto serialization = column_type->getSerialization(*info);
        column = column_type->createColumn(*serialization);

        DB::ISerialization::DeserializeBinaryBulkSettings settings;
        settings.getter = [&](DB::ISerialization::SubstreamPath) -> DB::ReadBuffer * { return &rb; };
        settings.avg_value_size_hint = 0;
        settings.position_independent_encoding = false;
        settings.native_format = true;

        DB::ISerialization::DeserializeBinaryBulkStatePtr state;

        serialization->deserializeBinaryBulkStatePrefix(settings, state);
        serialization->deserializeBinaryBulkWithMultipleStreams(column, rows, settings, state, nullptr);
        if (column->size() != rows)
            throw DB::Exception(
                DB::ErrorCodes::CANNOT_READ_ALL_DATA, "Cannot read all data. Rows read: {}. Rows expected: {}", column->size(), rows);
    }

    /// Cache prev-columns + current-column
    class ColumnsCache final
    {
    private:
        /// NOTE: The maxinum of cached ColumnsWithIndex size is `1073741824 * (sizeof(size_t) + sizeof(ColumnPtr))` bytes
        /// Default alloctor will tracking by `CurrentMemoryTracker::allocNoThrow()`, which means that the memory will be unlimited
        /// Fixed here by using 'AllocatorWithMemoryTracking' -> `CurrentMemoryTracker::alloc()`
        using ColumnsWithIndex = std::deque<std::pair<size_t, ColumnPtr>, AllocatorWithMemoryTracking<std::pair<size_t, ColumnPtr>>>;
        using CachedColumnsPair = std::pair<Int64, ColumnsWithIndex>;
        const Int64 max_prev_cache_rows = 0;
        DataTypePtr column_type;
        ColumnsWithIndex columns;
        Int64 curr_cache_rows = 0;
        mutable std::mutex mutex;

    public:
        ColumnsCache(size_t max_prev_cache_rows_, const DataTypePtr & column_type_)
            : max_prev_cache_rows(max_prev_cache_rows_), column_type(column_type_)
        {
        }

        CachedColumnsPair add(ColumnPtr column)
        {
            std::lock_guard lock(mutex);
            /// If prev columns cache full, we shall remove overflowing and useless column and/or set a valid begin_cursor
            Int64 overflow_size = curr_cache_rows - max_prev_cache_rows;
            while (overflow_size > 0)
            {
                auto & front_column = columns.front();
                if (front_column.second->size() > size_t(overflow_size))
                {
                    /// For example offset = -1, count = 1 (max_prev_cache_rows = 1), but there is one cached prev block that has 3 rows => `overflow_size = 2`
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

            return {curr_cache_rows, columns};
        }

        void serialize(WriteBuffer & wb) const
        {
            std::lock_guard lock(mutex);
            writeIntBinary(columns.size(), wb);
            for (const auto & [begin_offset, column] : columns)
            {
                writeIntBinary(begin_offset, wb);
                writeColumn(column, column_type, wb);
            }

            writeIntBinary(curr_cache_rows, wb);
        }

        void deserialize(ReadBuffer & rb)
        {
            std::lock_guard lock(mutex);
            size_t columns_num;
            readIntBinary(columns_num, rb);
            for (size_t i = 0; i < columns_num; ++i)
            {
                size_t begin_offset;
                ColumnPtr column;
                readIntBinary(begin_offset, rb);
                readColumn(column, column_type, rb);

                columns.emplace_back(std::move(begin_offset), std::move(column));
            }

            readIntBinary(curr_cache_rows, rb);
        }
    };

    template<typename Name>
    DataTypePtr checkAndGetReturnType(const DataTypes & arguments)
    {
        size_t number_of_arguments = arguments.size();

        if constexpr (std::is_same_v<Name, NameToLags>)
        {
            /// lags(column, begin_offset, end_offset[, default_value])
            if (number_of_arguments < 3 || number_of_arguments > 4)
                throw Exception(
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Number of arguments for function '{}' doesn't match: passed {}, should be from 3 or 4",
                    Name::name,
                    number_of_arguments);

            /// So far, we don't support operations of json column
            if (isObject(arguments[0]))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "No support type {} of argument 1 of function '{}'",
                    arguments[0]->getName(),
                    Name::name);

            /// Second/Third arguments must be integer
            for (int i = 1; i < 3; ++i)
            {
                if (!isInteger(arguments[i]))
                    throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Illegal type {} of argument {} of function '{}' - should be an integer",
                        arguments[i]->getName(),
                        i,
                        Name::name);
                else if (arguments[i]->isNullable())
                    throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Illegal type {} of argument {} of function '{}' - can not be Nullable",
                        arguments[i]->getName(),
                        i,
                        Name::name);
            }

            // check that default value column has supertype with first argument
            if (number_of_arguments == 4)
                return std::make_shared<DataTypeArray>(getLeastSupertype(DataTypes{arguments[0], arguments[3]}));

            return std::make_shared<DataTypeArray>(arguments[0]);
        }
        else
        {
            if constexpr (std::is_same_v<Name, NameToLag>)
            {
                /// lag(column[, offset = 1, default_value])
                if (number_of_arguments < 1 || number_of_arguments > 3)
                    throw Exception(
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Number of arguments for function '{}' doesn't match: passed {}, should be from 1 or 2 or 3",
                        Name::name,
                        number_of_arguments);
            }
            else
            {
                /// neighbor(column, offset[, default_value])
                if (number_of_arguments < 2 || number_of_arguments > 3)
                    throw Exception(
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Number of arguments for function '{}' doesn't match: passed {}, should be from 2 or 3",
                        Name::name,
                        number_of_arguments);
            }

            /// So far, we don't support operations of json column
            if (isObject(arguments[0]))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "No support type {} of argument 1 of function '{}'",
                    arguments[0]->getName(),
                    Name::name);

            // second argument must be an integer
            if (number_of_arguments >= 2)
            {
                if (!isInteger(arguments[1]))
                    throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Illegal type {} of second argument of function '{}' - should be an integer",
                        arguments[1]->getName(),
                        Name::name);
                else if (arguments[1]->isNullable())
                    throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Illegal type  {} of second argument of function '{}' - can not be Nullable",
                        arguments[1]->getName(),
                        Name::name);
            }

            // check that default value column has supertype with first argument
            if (number_of_arguments == 3)
                return getLeastSupertype(DataTypes{arguments[0], arguments[2]});

            return arguments[0];
        }
    }

    /// Specified streaming query, Limits:
    /// 1) The offset argument must be constant non-positive number (i.e. only reach current or prev rows)
    /// Example:
    /// SELECT c1, __streaming_neighbor(c1, -1) as c2, lag(c1, 1) as c3, lags(c1, 1, 3) as c4:
    /// (Input)             (Output)
    ///  <<<                    >>>
    /// | c1 |              | c1 | c2 | c3 |      c4      |
    /// | 10 |              | 10 | 0  | 0  | [0, 0, 0]    |
    /// | 20 |              | 20 | 10 | 10 | [10, 0, 0]   |
    ///
    ///  <<<                    >>>
    /// | c1 |              | c1 | c2 | c3 |      c4      |
    /// | 30 |              | 30 | 20 | 20 | [20, 10, 0]  |
    /// | 40 |              | 40 | 30 | 30 | [30, 20, 10] |

    template<typename Name>
    class FunctionStreamingNeighbor : public IFunction
    {
    private:
        UInt32 prev_offset = 0;
        Int64 count = 1;
        mutable ColumnsCache prev_offset_columns;

    public:
        explicit FunctionStreamingNeighbor(Int64 offset_, Int64 count_, const DataTypePtr & cached_column_type)
            : prev_offset(static_cast<UInt32>(std::abs(offset_)))
            , count(count_)
            , prev_offset_columns(prev_offset + count - 1, cached_column_type)
        {
            /// Protection from possible overflow.
            if constexpr (std::is_same_v<Name, NameToLag> || std::is_same_v<Name, NameToLags>)
            {
                /// lag [1, 1073741824]
                if (offset_ <= 0 || offset_ > (1 << 30))
                    throw Exception(
                        ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                        "Invalid offset: {} in function {}, expected [1, {}]",
                        offset_,
                        Name::name,
                        1 << 30);

                if (count_ <= 0 || count_ > (1 << 30))
                    throw Exception(
                        ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                        "Invalid offset: {} in function {}, expected [1, {}]",
                        offset_,
                        Name::name,
                        1 << 30);
            }
            else
            {
                /// neighbor [-1073741824, -1]
                if (offset_ >= 0 || offset_ < -(1 << 30))
                    throw Exception(
                        ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                        "Invalid offset: {} in function {}, expected [{}, -1]",
                        offset_,
                        Name::name,
                        -(1 << 30));
            }
        }

        /// Get the name of the function.
        String getName() const override { return Name::name; }

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

        DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override { return checkAndGetReturnType<Name>(arguments); }

        void insertIntoPrevColumn(
            MutableColumnPtr & result_column,
            const ColumnPtr & source_column_casted,
            size_t input_rows_count,
            bool default_is_constant,
            const ColumnPtr & default_column_casted) const
        {
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

            auto [total_cached_rows, columns_with_index] = prev_offset_columns.add(source_column_casted);

            /// insert default
            Int64 no_cached_rows = prev_offset + input_rows_count - total_cached_rows;
            if (no_cached_rows > 0)
                insert_range_from(default_is_constant, default_column_casted, 0, no_cached_rows);

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
        }

        void insertIntoPrevColumns(
            MutableColumnPtr & result_column,
            const ColumnPtr & source_column_casted,
            size_t input_rows_count,
            bool default_is_constant,
            const ColumnPtr & default_column_casted) const
        {
            auto [total_cached_rows, columns_with_index] = prev_offset_columns.add(source_column_casted);
            Int64 no_cached_rows = prev_offset + count - 1 + input_rows_count - total_cached_rows;
            size_t default_rows = no_cached_rows < 0 ? 0 : no_cached_rows;
            auto & array = assert_cast<ColumnArray &>(*result_column.get());
            auto & offsets = array.getOffsets();
            auto & data = array.getData();

            /// For examples - lags(column, 3, 5) => prev_offset = 3, count = 3:
            /// In prev_offset_columns cache
            ///     0: prev-5-row
            ///     1: prev-4-row
            ///     2: prev-3-row
            ///     3: prev-2-row
            ///     4: prev-1-row
            ///     5: current-row
            ///     ...
            /// current-row result = [prev-3-row, prev-4-row, prev-5-row]
            for (size_t row_index = 0; row_index < input_rows_count; ++row_index)
            {
                offsets.push_back(offsets.back() + count);
                for (Int64 i = 0; i < count; ++i)
                {
                    size_t elem_index = row_index + count - 1 - i;
                    assert(elem_index >= 0);
                    /// insert default value if no cache
                    if (elem_index < default_rows)
                    {
                        if (!default_column_casted)
                            data.insertDefault();
                        else if (default_is_constant)
                            data.insertFrom(*default_column_casted, 0);
                        else
                            data.insertFrom(*default_column_casted, row_index);
                        continue;
                    }

                    /// insert prev value
                    size_t index_in_cache = elem_index - default_rows;
                    assert(index_in_cache >= 0);
                    for (auto [begin, column] : columns_with_index)
                    {
                        size_t curr_column_cache_rows = column->size() - begin;
                        if (index_in_cache >= curr_column_cache_rows)
                        {
                            index_in_cache -= curr_column_cache_rows;
                            continue;  /// next cached column
                        }

                        bool is_constant = isColumnConst(*column);
                        if (is_constant)
                            column = assert_cast<const ColumnConst &>(*column).getDataColumnPtr();

                        if (is_constant)
                            data.insertFrom(*column, 0);
                        else
                            data.insertFrom(*column, begin + index_in_cache);
                        break;
                    }
                }
            }
        }

        ColumnPtr
        executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
        {
            const ColumnWithTypeAndName & source_elem = arguments[0];
            ColumnPtr source_column_casted;
            if constexpr (std::is_same_v<NameToLags, Name>)
            {
                source_column_casted = castColumn(source_elem, assert_cast<const DataTypeArray &>(*result_type.get()).getNestedType());
                if (input_rows_count == 0)
                    return ColumnArray::create(source_column_casted);
            }
            else
            {
                source_column_casted = castColumn(source_elem, result_type);
                /// Degenerate case, just copy source column as is.
                if (input_rows_count == 0)
                    return source_column_casted;
            }

            auto result_column = result_type->createColumn();
            ColumnPtr default_column_casted;
            bool default_is_constant = false;
            if constexpr (std::is_same_v<NameToLags, Name>)
            {
                /// lags(column, begin_offset, end_offset, [default_value])
                bool has_defaults = arguments.size() == 4;

                if (has_defaults)
                {
                    default_column_casted = castColumn(arguments[3], assert_cast<const DataTypeArray &>(*result_type.get()).getNestedType());
                    default_is_constant = isColumnConst(*default_column_casted);
                }

                if (default_is_constant)
                    default_column_casted = assert_cast<const ColumnConst &>(*default_column_casted).getDataColumnPtr();

                insertIntoPrevColumns(result_column, source_column_casted, input_rows_count, default_is_constant, default_column_casted);
            }
            else
            {
                /// __streaming_neighbor(column, offset, [default_value])
                /// lag(column, [offset = 1, default_value])
                bool has_defaults = arguments.size() == 3;

                if (has_defaults)
                {
                    default_column_casted = castColumn(arguments[2], result_type);
                    default_is_constant = isColumnConst(*default_column_casted);
                }

                if (default_is_constant)
                    default_column_casted = assert_cast<const ColumnConst &>(*default_column_casted).getDataColumnPtr();

                insertIntoPrevColumn(result_column, source_column_casted, input_rows_count, default_is_constant, default_column_casted);
            }

            assert(result_column->size() == input_rows_count);

            return result_column;
        }

        void serialize(WriteBuffer & wb) const override
        {
            prev_offset_columns.serialize(wb);
        }

        void deserialize(ReadBuffer & rb) const override
        {
            prev_offset_columns.deserialize(rb);
        }
    };

    template<typename Name>
    class NeighborOverloadResolver : public IFunctionOverloadResolver
    {
    public:
        static FunctionOverloadResolverPtr create(ContextPtr)
        {
            return std::make_unique<NeighborOverloadResolver>();
        }

        NeighborOverloadResolver() { }

        String getName() const override { return Name::name; }
        size_t getNumberOfArguments() const override { return 0; }
        bool isVariadic() const override { return true; }
        bool isStateful() const override { return true; }
        bool isDeterministic() const override { return false; }
        bool isDeterministicInScopeOfQuery() const override { return false; }
        bool useDefaultImplementationForNulls() const override { return false; }

        /// We do not use default implementation for LowCardinality because this is not a pure function.
        /// If used, optimization for LC may execute function only for dictionary, which gives wrong result.
        bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

        FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
        {
            /// Check ahead in `getReturnTypeImpl`
            /// __streaming_neighbor(column, offset, [default_value])
            /// lag(column, [offset = 1, default_value])
            /// lags(column, begin_offset, end_offset, [default_value])
            Int64 offset = 1;
            if (arguments.size() > 1)
            {
                const auto * offset_col = checkAndGetColumn<ColumnConst>(arguments[1].column.get());
                if (!offset_col)
                    throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Invalid offset, only support constant offset in current query. '{}'", getName());
                offset = offset_col->getInt(0);
            }

            Int64 count = 1;
            if constexpr (std::is_same_v<Name, NameToLags>)
            {
                assert(arguments.size() > 2);
                const auto * offset_col = checkAndGetColumn<ColumnConst>(arguments[2].column.get());
                if (!offset_col)
                    throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Invalid offset, only support constant offset in current query. '{}'", getName());
                count = offset_col->getInt(0) - offset + 1;
            }

            return std::make_unique<FunctionToFunctionBaseAdaptor>(
                std::make_shared<FunctionStreamingNeighbor<Name>>(offset, count, arguments[0].type),
                collections::map<DataTypes>(arguments, [](const auto & elem) { return elem.type; }),
                return_type);
        }

        DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override { return checkAndGetReturnType<Name>(arguments); }
    };
}

REGISTER_FUNCTION(StreamingNeighbor)
{
    factory.registerFunction<NeighborOverloadResolver<NameToStreamingNeighbor>>("__streaming_neighbor");
    factory.registerFunction<NeighborOverloadResolver<NameToLag>>("lag");
    factory.registerFunction<NeighborOverloadResolver<NameToLags>>("lags");
}

}
