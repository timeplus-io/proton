#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <base/map.h>
#include <Common/HashTable/Hash.h>
#include <Common/HashTable/HashSet.h>
#include <Common/SipHash.h>

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
    struct KeySet
    {
        KeySet(UInt32 max_entries_, bool last_arg_is_limit_) : max_entries(max_entries_), last_arg_is_limit(last_arg_is_limit_) { }

        /// FIXME, if dedup column is integer, use them directly
        UInt32 max_entries;
        using Key = UInt128;

        /// When creating, the hash table must be small.
        using Set = HashSet<Key, UInt128TrivialHash, HashTableGrower<3>, HashTableAllocatorWithStackMemory<sizeof(Key) * (1 << 3)>>;

        Set key_set;

        bool last_arg_is_limit;

        std::deque<UInt128> keys;

        mutable std::mutex mutex;

        ColumnPtr populateKeySetsAndCalculateResults(const ColumnsWithTypeAndName & arguments, size_t rows, MutableColumnPtr result)
        {
            size_t nargs = arguments.size() - last_arg_is_limit;

            if (last_arg_is_limit)
                assert_cast<const ColumnConst*>(arguments.back().column.get());

            std::scoped_lock lock(mutex);

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
                auto [_, inserted] = key_set.insert(key);
                if (inserted)
                {
                    result->insert(1);
                    keys.push_back(key);
                }
                else
                    result->insert(0);
            }

            while (keys.size() > max_entries)
            {
                key_set.erase(keys.front());
                keys.pop_front();
            }

            return result;
        }
    };

    DataTypePtr checkAndGetReturnType(const DataTypes & arguments, const String & func_name)
    {
        if (arguments.empty())
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function '{}' doesn't match: passed 0, should be at least 1",
                func_name);


        for (const auto & arg_type : arguments)
            if (!isInteger(arg_type) && !isString(arg_type) && !isFloat(arg_type))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "The types of deduplication keys of function '{}' shall be integer, float or string",
                    func_name);

        return std::make_shared<DB::DataTypeUInt8>();
    }

    class FunctionDedup : public IFunction
    {
    public:
        static constexpr auto name = "__dedup";

        FunctionDedup(UInt32 max_entries, bool last_arg_is_limit) : key_set(max_entries, last_arg_is_limit) { }

        String getName() const override { return name; }

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

        DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override { return checkAndGetReturnType(arguments, name); }

        ColumnPtr
        executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
        {
            MutableColumnPtr result{result_type->createColumn()};
            if (input_rows_count == 0)
                return result;

            auto * col = assert_cast<ColumnVector<UInt8> *>(result.get());
            col->reserve(input_rows_count);

            return key_set.populateKeySetsAndCalculateResults(arguments, input_rows_count, std::move(result));
        }

    private:
        mutable KeySet key_set;
    };

    class DedupOverloadResolver : public IFunctionOverloadResolver
    {
    public:
        static FunctionOverloadResolverPtr create(ContextPtr) { return std::make_unique<DedupOverloadResolver>(); }

        DedupOverloadResolver() { }

        String getName() const override { return FunctionDedup::name; }
        size_t getNumberOfArguments() const override { return 0; }
        bool isVariadic() const override { return true; }

        FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
        {
            UInt32 limit = 10000;
            bool last_arg_is_limit = false;

            /// dedup(column1, column2, ..., limit)
            if (arguments.size() > 1)
            {
                const auto * limit_col = checkAndGetColumn<ColumnConst>(arguments.back().column.get());
                if (limit_col)
                {
                    limit = limit_col->getUInt(0);
                    last_arg_is_limit = true;
                }
            }

            return std::make_unique<FunctionToFunctionBaseAdaptor>(
                std::make_shared<FunctionDedup>(limit, last_arg_is_limit),
                collections::map<DataTypes>(arguments, [](const auto & elem) { return elem.type; }),
                return_type);
        }

        DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
        {
            return checkAndGetReturnType(arguments, FunctionDedup::name);
        }
    };
}

void registerFunctionDedup(FunctionFactory & factory)
{
    factory.registerFunction<DedupOverloadResolver>(FunctionDedup::name, FunctionFactory::CaseSensitive);
}

}
