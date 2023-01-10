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
    KeySet(UInt32 max_entries_, Int32 timeout_sec_, UInt32 limit_args_)
        : limit_args(limit_args_), max_entries(max_entries_), timeout_interval_ms(timeout_sec_ * 1000)
    {
    }

    ColumnPtr populateKeySetsAndCalculateResults(const ColumnsWithTypeAndName & arguments, size_t rows, MutableColumnPtr result)
    {
        size_t nargs = arguments.size() - limit_args;

        std::vector<UInt128> hash_keys;
        hash_keys.reserve(rows);

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
            hash_keys.push_back(key);
        }

        {
            std::scoped_lock lock(mutex);

            /// Remove keys which reach limits
            while (keys.size() > max_entries)
            {
                key_set.erase(keys.front().key);
                keys.pop_front();
            }

            /// Remove keys which reach timeout
            while (!keys.empty() && keys.front().timedOut(timeout_interval_ms))
            {
                key_set.erase(keys.front().key);
                keys.pop_front();
            }

            for (const auto & key : hash_keys)
            {
                auto [_, inserted] = key_set.insert(key);
                if (inserted)
                {
                    result->insert(1);
                    keys.emplace_back(key);
                }
                else
                    result->insert(0);
            }
        }

        return result;
    }

private:
    /// FIXME, if dedup column is integer, use them directly
    UInt32 limit_args;
    UInt32 max_entries;
    Int32 timeout_interval_ms;

    using Key = UInt128;

    /// When creating, the hash table must be small.
    using Set = HashSet<Key, UInt128TrivialHash, HashTableGrower<3>, HashTableAllocatorWithStackMemory<sizeof(Key) * (1 << 3)>>;

    Set key_set;

    struct KeyTime
    {
        explicit KeyTime(UInt128 key_) : key(key_), time(MonotonicMilliseconds::now()) { }

        bool timedOut(Int32 timeout_) const { return timeout_ > 0 && MonotonicMilliseconds::now() - time > timeout_; }

        UInt128 key;
        Int64 time;
    };

    std::deque<KeyTime> keys;

    mutable std::mutex mutex;
};

DataTypePtr checkAndGetReturnType(const DataTypes & arguments, const String & func_name)
{
    if (arguments.empty())
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Number of arguments for function '{}' doesn't match: passed 0, should be at least 1",
            func_name);

    for (const auto & arg_type : arguments)
        if (!isColumnedAsNumber(arg_type) && !isStringOrFixedString(arg_type) && !isColumnedAsDecimal(arg_type) && !isInterval(arg_type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The types of deduplication keys of function '{}' shall be integer, float, datetime, decimal or string but got={}",
                func_name, arg_type->getName());

    return std::make_shared<DB::DataTypeUInt8>();
}

/// dedup(column1, column2, ..., [limit, [timeout]])
class FunctionDedup : public IFunction
{
public:
    static constexpr auto name = "__dedup";

    FunctionDedup(UInt32 max_entries, Int32 timeout, UInt32 limit_args) : key_set(max_entries, timeout, limit_args) { }

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

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override { return checkAndGetReturnType(arguments, "dedup"); }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
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
        Int32 limit_sec = -1;
        UInt32 limit_args = 0;

        /// dedup(column1, column2, ..., [timeout, [limit]])

        auto check_limit = [&](size_t pos) {
            const auto * limit_col = checkAndGetColumn<ColumnConst>(arguments[pos].column.get());
            if (limit_col && isInteger(arguments[pos].type))
            {
                limit = static_cast<UInt32>(limit_col->getUInt(0));
                ++limit_args;
                return true;
            }
            return false;
        };

        auto check_timeout = [&](size_t pos) {
            const auto * limit_col = checkAndGetColumn<ColumnConst>(arguments[pos].column.get());
            if (limit_col && isInterval(arguments[pos].type))
            {
                limit_sec = static_cast<Int32>(limit_col->getInt(0));
                ++limit_args;
            }
        };

        if (arguments.size() > 2)
        {
            /// Check if last parameter is interval
            if (check_limit(arguments.size() - 1))
                /// Check if the second last is number limit
                check_timeout(arguments.size() - 2);
        }
        else if (arguments.size() == 2)
        {
            check_timeout(arguments.size() - 1);
        }

        return std::make_unique<FunctionToFunctionBaseAdaptor>(
            std::make_shared<FunctionDedup>(limit, limit_sec, limit_args),
            collections::map<DataTypes>(arguments, [](const auto & elem) { return elem.type; }),
            return_type);
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        return checkAndGetReturnType(arguments, "dedup");
    }
};
}

REGISTER_FUNCTION(Dedup)
{
    factory.registerFunction<DedupOverloadResolver>(FunctionDedup::name);
}

}
