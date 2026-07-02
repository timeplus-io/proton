#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/ITupleFunction.h>
#include "Functions/IFunction.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

struct PlusName { static constexpr auto name = "plus"; };
struct MinusName { static constexpr auto name = "minus"; };
struct MultiplyName { static constexpr auto name = "multiply"; };
struct DivideName { static constexpr auto name = "divide"; };

template <class FuncName>
class FunctionTupleOperator : public ITupleFunction
{
public:
    /// constexpr cannot be used due to std::string has not constexpr constructor in this compiler version
    static inline auto name = std::string("tuple_") + FuncName::name;

    explicit FunctionTupleOperator(ContextPtr context_) : ITupleFunction(context_) {}
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionTupleOperator>(context_); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const auto * left_tuple = checkAndGetDataType<DataTypeTuple>(arguments[0].type.get());
        const auto * right_tuple = checkAndGetDataType<DataTypeTuple>(arguments[1].type.get());

        if (!left_tuple)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument 0 of function {} should be tuple, got {}",
                            getName(), arguments[0].type->getName());

        if (!right_tuple)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument 1 of function {} should be tuple, got {}",
                            getName(), arguments[1].type->getName());

        const auto & left_types = left_tuple->getElements();
        const auto & right_types = right_tuple->getElements();

        Columns left_elements;
        Columns right_elements;
        if (arguments[0].column)
            left_elements = getTupleElements(*arguments[0].column);
        if (arguments[1].column)
            right_elements = getTupleElements(*arguments[1].column);

        if (left_types.size() != right_types.size())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "Expected tuples of the same size as arguments of function {}. Got {} and {}",
                            getName(), arguments[0].type->getName(), arguments[1].type->getName());

        size_t tuple_size = left_types.size();
        if (tuple_size == 0)
            return std::make_shared<DataTypeUInt8>();

        auto func = FunctionFactory::instance().get(FuncName::name, context);
        DataTypes types(tuple_size);
        for (size_t i = 0; i < tuple_size; ++i)
        {
            try
            {
                ColumnWithTypeAndName left{left_elements.empty() ? nullptr : left_elements[i], left_types[i], {}};
                ColumnWithTypeAndName right{right_elements.empty() ? nullptr : right_elements[i], right_types[i], {}};
                auto elem_func = func->build(ColumnsWithTypeAndName{left, right});
                types[i] = elem_func->getResultType();
            }
            catch (DB::Exception & e)
            {
                e.addMessage("While executing function {} for tuple element {}", getName(), i);
                throw;
            }
        }

        return std::make_shared<DataTypeTuple>(types);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * left_tuple = checkAndGetDataType<DataTypeTuple>(arguments[0].type.get());
        const auto * right_tuple = checkAndGetDataType<DataTypeTuple>(arguments[1].type.get());
        const auto & left_types = left_tuple->getElements();
        const auto & right_types = right_tuple->getElements();
        auto left_elements = getTupleElements(*arguments[0].column);
        auto right_elements = getTupleElements(*arguments[1].column);

        size_t tuple_size = left_elements.size();
        if (tuple_size == 0)
            return DataTypeUInt8().createColumnConstWithDefaultValue(input_rows_count);

        auto func = FunctionFactory::instance().get(FuncName::name, context);
        Columns columns(tuple_size);
        for (size_t i = 0; i < tuple_size; ++i)
        {
            ColumnWithTypeAndName left{left_elements[i], left_types[i], {}};
            ColumnWithTypeAndName right{right_elements[i], right_types[i], {}};
            auto elem_func = func->build(ColumnsWithTypeAndName{left, right});
            columns[i] = elem_func->execute({left, right}, elem_func->getResultType(), input_rows_count)
                                  ->convertToFullColumnIfConst();
        }

        return ColumnTuple::create(columns);
    }
};

using FunctionTuplePlus = FunctionTupleOperator<PlusName>;

using FunctionTupleMinus = FunctionTupleOperator<MinusName>;

using FunctionTupleMultiply = FunctionTupleOperator<MultiplyName>;

using FunctionTupleDivide = FunctionTupleOperator<DivideName>;

class FunctionTupleNegate : public ITupleFunction
{
public:
    static constexpr auto name = "tuple_negate";

    explicit FunctionTupleNegate(ContextPtr context_) : ITupleFunction(context_) {}
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionTupleNegate>(context_); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const auto * cur_tuple = checkAndGetDataType<DataTypeTuple>(arguments[0].type.get());

        if (!cur_tuple)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument 0 of function {} should be tuple, got {}",
                            getName(), arguments[0].type->getName());

        const auto & cur_types = cur_tuple->getElements();

        Columns cur_elements;
        if (arguments[0].column)
            cur_elements = getTupleElements(*arguments[0].column);

        size_t tuple_size = cur_types.size();
        if (tuple_size == 0)
            return std::make_shared<DataTypeUInt8>();

        auto negate = FunctionFactory::instance().get("negate", context);
        DataTypes types(tuple_size);
        for (size_t i = 0; i < tuple_size; ++i)
        {
            try
            {
                ColumnWithTypeAndName cur{cur_elements.empty() ? nullptr : cur_elements[i], cur_types[i], {}};
                auto elem_negate = negate->build(ColumnsWithTypeAndName{cur});
                types[i] = elem_negate->getResultType();
            }
            catch (DB::Exception & e)
            {
                e.addMessage("While executing function {} for tuple element {}", getName(), i);
                throw;
            }
        }

        return std::make_shared<DataTypeTuple>(types);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * cur_tuple = checkAndGetDataType<DataTypeTuple>(arguments[0].type.get());
        const auto & cur_types = cur_tuple->getElements();
        auto cur_elements = getTupleElements(*arguments[0].column);

        size_t tuple_size = cur_elements.size();
        if (tuple_size == 0)
            return DataTypeUInt8().createColumnConstWithDefaultValue(input_rows_count);

        auto negate = FunctionFactory::instance().get("negate", context);
        Columns columns(tuple_size);
        for (size_t i = 0; i < tuple_size; ++i)
        {
            ColumnWithTypeAndName cur{cur_elements[i], cur_types[i], {}};
            auto elem_negate = negate->build(ColumnsWithTypeAndName{cur});
            columns[i] = elem_negate->execute({cur}, elem_negate->getResultType(), input_rows_count)
                                    ->convertToFullColumnIfConst();
        }

        return ColumnTuple::create(columns);
    }
};

template <class FuncName>
class FunctionTupleOperatorByNumber : public ITupleFunction
{
public:
    /// constexpr cannot be used due to std::string has not constexpr constructor in this compiler version
    static inline auto name = std::string("tuple_") + FuncName::name + "_by_number";

    explicit FunctionTupleOperatorByNumber(ContextPtr context_) : ITupleFunction(context_) {}
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionTupleOperatorByNumber>(context_); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const auto * cur_tuple = checkAndGetDataType<DataTypeTuple>(arguments[0].type.get());

        if (!cur_tuple)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument 0 of function {} should be tuple, got {}",
                            getName(), arguments[0].type->getName());

        const auto & cur_types = cur_tuple->getElements();

        Columns cur_elements;
        if (arguments[0].column)
            cur_elements = getTupleElements(*arguments[0].column);

        size_t tuple_size = cur_types.size();
        if (tuple_size == 0)
            return std::make_shared<DataTypeUInt8>();

        const auto & p_column = arguments[1];
        auto func = FunctionFactory::instance().get(FuncName::name, context);
        DataTypes types(tuple_size);
        for (size_t i = 0; i < tuple_size; ++i)
        {
            try
            {
                ColumnWithTypeAndName cur{cur_elements.empty() ? nullptr : cur_elements[i], cur_types[i], {}};
                auto elem_func = func->build(ColumnsWithTypeAndName{cur, p_column});
                types[i] = elem_func->getResultType();
            }
            catch (DB::Exception & e)
            {
                e.addMessage("While executing function {} for tuple element {}", getName(), i);
                throw;
            }
        }

        return std::make_shared<DataTypeTuple>(types);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * cur_tuple = checkAndGetDataType<DataTypeTuple>(arguments[0].type.get());
        const auto & cur_types = cur_tuple->getElements();
        auto cur_elements = getTupleElements(*arguments[0].column);

        size_t tuple_size = cur_elements.size();
        if (tuple_size == 0)
            return DataTypeUInt8().createColumnConstWithDefaultValue(input_rows_count);

        const auto & p_column = arguments[1];
        auto func = FunctionFactory::instance().get(FuncName::name, context);
        Columns columns(tuple_size);
        for (size_t i = 0; i < tuple_size; ++i)
        {
            ColumnWithTypeAndName cur{cur_elements[i], cur_types[i], {}};
            auto elem_func = func->build(ColumnsWithTypeAndName{cur, p_column});
            columns[i] = elem_func->execute({cur, p_column}, elem_func->getResultType(), input_rows_count)
                                  ->convertToFullColumnIfConst();
        }

        return ColumnTuple::create(columns);
    }
};

using FunctionTupleMultiplyByNumber = FunctionTupleOperatorByNumber<MultiplyName>;

using FunctionTupleDivideByNumber = FunctionTupleOperatorByNumber<DivideName>;

class FunctionDotProduct : public ITupleFunction
{
public:
    static constexpr auto name = "dot_product";

    explicit FunctionDotProduct(ContextPtr context_) : ITupleFunction(context_) {}
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionDotProduct>(context_); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const auto * left_tuple = checkAndGetDataType<DataTypeTuple>(arguments[0].type.get());
        const auto * right_tuple = checkAndGetDataType<DataTypeTuple>(arguments[1].type.get());

        if (!left_tuple)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument 0 of function {} should be tuple, got {}",
                            getName(), arguments[0].type->getName());

        if (!right_tuple)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument 1 of function {} should be tuple, got {}",
                            getName(), arguments[1].type->getName());

        const auto & left_types = left_tuple->getElements();
        const auto & right_types = right_tuple->getElements();

        Columns left_elements;
        Columns right_elements;
        if (arguments[0].column)
            left_elements = getTupleElements(*arguments[0].column);
        if (arguments[1].column)
            right_elements = getTupleElements(*arguments[1].column);

        if (left_types.size() != right_types.size())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "Expected tuples of the same size as arguments of function {}. Got {} and {}",
                            getName(), arguments[0].type->getName(), arguments[1].type->getName());

        size_t tuple_size = left_types.size();
        if (tuple_size == 0)
            return std::make_shared<DataTypeUInt8>();

        auto multiply = FunctionFactory::instance().get("multiply", context);
        auto plus = FunctionFactory::instance().get("plus", context);
        DataTypePtr res_type;
        for (size_t i = 0; i < tuple_size; ++i)
        {
            try
            {
                ColumnWithTypeAndName left{left_elements.empty() ? nullptr : left_elements[i], left_types[i], {}};
                ColumnWithTypeAndName right{right_elements.empty() ? nullptr : right_elements[i], right_types[i], {}};
                auto elem_multiply = multiply->build(ColumnsWithTypeAndName{left, right});

                if (i == 0)
                {
                    res_type = elem_multiply->getResultType();
                    continue;
                }

                ColumnWithTypeAndName left_type{res_type, {}};
                ColumnWithTypeAndName right_type{elem_multiply->getResultType(), {}};
                auto plus_elem = plus->build({left_type, right_type});
                res_type = plus_elem->getResultType();
            }
            catch (DB::Exception & e)
            {
                e.addMessage("While executing function {} for tuple element {}", getName(), i);
                throw;
            }
        }

        return res_type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * left_tuple = checkAndGetDataType<DataTypeTuple>(arguments[0].type.get());
        const auto * right_tuple = checkAndGetDataType<DataTypeTuple>(arguments[1].type.get());
        const auto & left_types = left_tuple->getElements();
        const auto & right_types = right_tuple->getElements();
        auto left_elements = getTupleElements(*arguments[0].column);
        auto right_elements = getTupleElements(*arguments[1].column);

        size_t tuple_size = left_elements.size();
        if (tuple_size == 0)
            return DataTypeUInt8().createColumnConstWithDefaultValue(input_rows_count);

        auto multiply = FunctionFactory::instance().get("multiply", context);
        auto plus = FunctionFactory::instance().get("plus", context);
        ColumnWithTypeAndName res;
        for (size_t i = 0; i < tuple_size; ++i)
        {
            ColumnWithTypeAndName left{left_elements[i], left_types[i], {}};
            ColumnWithTypeAndName right{right_elements[i], right_types[i], {}};
            auto elem_multiply = multiply->build(ColumnsWithTypeAndName{left, right});

            ColumnWithTypeAndName column;
            column.type = elem_multiply->getResultType();
            column.column = elem_multiply->execute({left, right}, column.type, input_rows_count);

            if (i == 0)
            {
                res = std::move(column);
            }
            else
            {
                auto plus_elem = plus->build({res, column});
                auto res_type = plus_elem->getResultType();
                res.column = plus_elem->execute({res, column}, res_type, input_rows_count);
                res.type = res_type;
            }
        }

        return res.column;
    }
};

template <typename Impl>
class FunctionDateOrDateTimeOperationTupleOfIntervals : public ITupleFunction
{
public:
    static constexpr auto name = Impl::name;

    explicit FunctionDateOrDateTimeOperationTupleOfIntervals(ContextPtr context_) : ITupleFunction(context_) {}
    static FunctionPtr create(ContextPtr context_)
    {
        return std::make_shared<FunctionDateOrDateTimeOperationTupleOfIntervals>(context_);
    }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (!isDateOrDate32(arguments[0].type) && !isDateTime(arguments[0].type) && !isDateTime64(arguments[0].type))
                throw Exception{ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of first argument of function {}. Should be a date or a date with time",
                    arguments[0].type->getName(), getName()};

        const auto * cur_tuple = checkAndGetDataType<DataTypeTuple>(arguments[1].type.get());

        if (!cur_tuple)
            throw Exception{ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of second argument of function {}. Should be a tuple",
                    arguments[0].type->getName(), getName()};

        const auto & cur_types = cur_tuple->getElements();

        Columns cur_elements;
        if (arguments[1].column)
            cur_elements = getTupleElements(*arguments[1].column);

        size_t tuple_size = cur_types.size();
        if (tuple_size == 0)
            return arguments[0].type;

        auto plus = FunctionFactory::instance().get(Impl::func_name, context);
        DataTypePtr res_type = arguments[0].type;
        for (size_t i = 0; i < tuple_size; ++i)
        {
            try
            {
                ColumnWithTypeAndName left{res_type, {}};
                ColumnWithTypeAndName right{cur_elements.empty() ? nullptr : cur_elements[i], cur_types[i], {}};
                auto plus_elem = plus->build({left, right});
                res_type = plus_elem->getResultType();
            }
            catch (DB::Exception & e)
            {
                e.addMessage("While executing function {} for tuple element {}", getName(), i);
                throw;
            }
        }

        return res_type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * cur_tuple = checkAndGetDataType<DataTypeTuple>(arguments[1].type.get());
        const auto & cur_types = cur_tuple->getElements();
        auto cur_elements = getTupleElements(*arguments[1].column);

        size_t tuple_size = cur_elements.size();
        if (tuple_size == 0)
            return arguments[0].column;

        auto plus = FunctionFactory::instance().get(Impl::func_name, context);
        ColumnWithTypeAndName res;
        for (size_t i = 0; i < tuple_size; ++i)
        {
            ColumnWithTypeAndName column{cur_elements[i], cur_types[i], {}};
            auto elem_plus = plus->build(ColumnsWithTypeAndName{i == 0 ? arguments[0] : res, column});
            auto res_type = elem_plus->getResultType();
            res.column = elem_plus->execute({i == 0 ? arguments[0] : res, column}, res_type, input_rows_count);
            res.type = res_type;
        }

        return res.column;
    }
};

struct AddTupleOfIntervalsImpl
{
    static constexpr auto name = "add_tuple_of_intervals";
    static constexpr auto func_name = "plus";
};

struct SubtractTupleOfIntervalsImpl
{
    static constexpr auto name = "subtract_tuple_of_intervals";
    static constexpr auto func_name = "minus";
};

using FunctionAddTupleOfIntervals = FunctionDateOrDateTimeOperationTupleOfIntervals<AddTupleOfIntervalsImpl>;

using FunctionSubtractTupleOfIntervals = FunctionDateOrDateTimeOperationTupleOfIntervals<SubtractTupleOfIntervalsImpl>;

template <bool is_minus>
struct FunctionTupleOperationInterval : public ITupleFunction
{
public:
    static constexpr auto name = is_minus ? "subtract_interval" : "add_interval";

    explicit FunctionTupleOperationInterval(ContextPtr context_) : ITupleFunction(context_) {}

    static FunctionPtr create(ContextPtr context_)
    {
        return std::make_shared<FunctionTupleOperationInterval>(context_);
    }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isTuple(arguments[0]) && !isInterval(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of first argument of function {}, must be tuple or interval",
                arguments[0]->getName(), getName());

        if (!isInterval(arguments[1]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of second argument of function {}, must be interval",
                arguments[1]->getName(), getName());

        DataTypes types;

        const auto * tuple = checkAndGetDataType<DataTypeTuple>(arguments[0].get());

        if (tuple)
        {
            const auto & cur_types = tuple->getElements();

            for (const auto & type : cur_types)
                if (!isInterval(type))
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Illegal type {} of Tuple element of first argument of function {}, must be interval",
                        type->getName(), getName());

            types = cur_types;
        }
        else
        {
            types = {arguments[0]};
        }

        const auto * interval_last = checkAndGetDataType<DataTypeInterval>(types.back().get());
        const auto * interval_new = checkAndGetDataType<DataTypeInterval>(arguments[1].get());

        if (!interval_last->equals(*interval_new))
            types.push_back(arguments[1]);

        return std::make_shared<DataTypeTuple>(types);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        if (!isInterval(arguments[1].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of second argument of function {}, must be interval",
                arguments[1].type->getName(), getName());

        Columns tuple_columns;

        const auto * first_tuple = checkAndGetDataType<DataTypeTuple>(arguments[0].type.get());
        const auto * first_interval = checkAndGetDataType<DataTypeInterval>(arguments[0].type.get());
        const auto * second_interval = checkAndGetDataType<DataTypeInterval>(arguments[1].type.get());

        bool can_be_merged;

        if (first_interval)
        {
            can_be_merged = first_interval->equals(*second_interval);

            if (can_be_merged)
                tuple_columns.resize(1);
            else
                tuple_columns.resize(2);

            tuple_columns[0] = arguments[0].column->convertToFullColumnIfConst();
        }
        else if (first_tuple)
        {
            const auto & cur_types = first_tuple->getElements();

            for (const auto & type : cur_types)
                if (!isInterval(type))
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Illegal type {} of Tuple element of first argument of function {}, must be interval",
                        type->getName(), getName());

            auto cur_elements = getTupleElements(*arguments[0].column);
            size_t tuple_size = cur_elements.size();

            if (tuple_size == 0)
            {
                can_be_merged = false;
            }
            else
            {
                const auto * tuple_last_interval = checkAndGetDataType<DataTypeInterval>(cur_types.back().get());
                can_be_merged = tuple_last_interval->equals(*second_interval);
            }

            if (can_be_merged)
                tuple_columns.resize(tuple_size);
            else
                tuple_columns.resize(tuple_size + 1);

            for (size_t i = 0; i < tuple_size; ++i)
                tuple_columns[i] = cur_elements[i];
        }
        else
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of first argument of function {}, must be tuple or interval",
                arguments[0].type->getName(), getName());


        ColumnPtr & last_column = tuple_columns.back();

        if (can_be_merged)
        {
            ColumnWithTypeAndName left{last_column, arguments[1].type, {}};

            if constexpr (is_minus)
            {
                auto minus = FunctionFactory::instance().get("minus", context);
                auto elem_minus = minus->build({left, arguments[1]});
                last_column = elem_minus->execute({left, arguments[1]}, arguments[1].type, input_rows_count)
                                        ->convertToFullColumnIfConst();
            }
            else
            {
                auto plus = FunctionFactory::instance().get("plus", context);
                auto elem_plus = plus->build({left, arguments[1]});
                last_column = elem_plus->execute({left, arguments[1]}, arguments[1].type, input_rows_count)
                                        ->convertToFullColumnIfConst();
            }
        }
        else
        {
            if constexpr (is_minus)
            {
                auto negate = FunctionFactory::instance().get("negate", context);
                auto elem_negate = negate->build({arguments[1]});
                last_column = elem_negate->execute({arguments[1]}, arguments[1].type, input_rows_count);
            }
            else
            {
                last_column = arguments[1].column;
            }
        }

        return ColumnTuple::create(tuple_columns);
    }
};

using FunctionTupleAddInterval = FunctionTupleOperationInterval<false>;

using FunctionTupleSubtractInterval = FunctionTupleOperationInterval<true>;


REGISTER_FUNCTION(VectorFunctions)
{
    factory.registerFunction<FunctionTuplePlus>();
    factory.registerAlias("vector_sum", FunctionTuplePlus::name, FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionTupleMinus>();
    factory.registerAlias("vector_difference", FunctionTupleMinus::name, FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionTupleMultiply>();
    factory.registerFunction<FunctionTupleDivide>();
    factory.registerFunction<FunctionTupleNegate>();

    factory.registerFunction<FunctionAddTupleOfIntervals>(FunctionDocumentation
        {
            .description=R"(
Consecutively adds a tuple of intervals to a date or a datetime.
[example:tuple]
)",
            .examples{
                {"tuple", "WITH to_date('2018-01-01') AS date SELECT add_tuple_of_intervals(date, (INTERVAL 1 DAY, INTERVAL 1 YEAR))", ""},
                },
            .category{"Dates and Times"}
        });

    factory.registerFunction<FunctionSubtractTupleOfIntervals>(FunctionDocumentation
        {
            .description=R"(
Consecutively subtracts a tuple of intervals from a Date or a DateTime.
[example:tuple]
)",
            .examples{
                {"tuple", "WITH to_date('2018-01-01') AS date SELECT subtract_tuple_of_intervals(date, (INTERVAL 1 DAY, INTERVAL 1 YEAR))", ""},
                },
            .category{"Dates and Times"}
        });

    factory.registerFunction<FunctionTupleAddInterval>(FunctionDocumentation
        {
            .description=R"(
Adds an interval to another interval or tuple of intervals. The returned value is tuple of intervals.
[example:tuple]
[example:interval1]

If the types of the first interval (or the interval in the tuple) and the second interval are the same they will be merged into one interval.
[example:interval2]
)",
            .examples{
                {"tuple", "SELECT add_interval((INTERVAL 1 DAY, INTERVAL 1 YEAR), INTERVAL 1 MONTH)", ""},
                {"interval1", "SELECT add_interval(INTERVAL 1 DAY, INTERVAL 1 MONTH)", ""},
                {"interval2", "SELECT add_interval(INTERVAL 1 DAY, INTERVAL 1 DAY)", ""},
                },
            .category{"Dates and Times"}
        });
    factory.registerFunction<FunctionTupleSubtractInterval>(FunctionDocumentation
        {
            .description=R"(
Adds an negated interval to another interval or tuple of intervals. The returned value is tuple of intervals.
[example:tuple]
[example:interval1]

If the types of the first interval (or the interval in the tuple) and the second interval are the same they will be merged into one interval.
[example:interval2]
)",
            .examples{
                {"tuple", "SELECT subtract_interval((INTERVAL 1 DAY, INTERVAL 1 YEAR), INTERVAL 1 MONTH)", ""},
                {"interval1", "SELECT subtract_interval(INTERVAL 1 DAY, INTERVAL 1 MONTH)", ""},
                {"interval2", "SELECT subtract_interval(INTERVAL 2 DAY, INTERVAL 1 DAY)", ""},
                },
            .category{"Dates and Times"}
        });

    factory.registerFunction<FunctionTupleMultiplyByNumber>();
    factory.registerFunction<FunctionTupleDivideByNumber>();

    factory.registerFunction<FunctionDotProduct>();
    factory.registerAlias("scalar_product", FunctionDotProduct::name, FunctionFactory::CaseInsensitive);
}
}
