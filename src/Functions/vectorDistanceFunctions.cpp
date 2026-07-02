#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/ITupleFunction.h>
#include <Functions/castTypeToEither.h>
#include "Functions/IFunction.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int ARGUMENT_OUT_OF_BOUND;
}

struct L1Label { static constexpr auto name = "1"; };
struct L2Label { static constexpr auto name = "2"; };
struct L2SquaredLabel { static constexpr auto name = "2_squared"; };
struct LinfLabel { static constexpr auto name = "inf"; };
struct LpLabel { static constexpr auto name = "p"; };

/// this is for convenient usage in LNormalize
template <class FuncLabel>
class FunctionLNorm : public ITupleFunction {};

template <>
class FunctionLNorm<L1Label> : public ITupleFunction
{
public:
    static constexpr auto name = "l1_norm";

    explicit FunctionLNorm(ContextPtr context_) : ITupleFunction(context_) {}
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionLNorm>(context_); }

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

        auto abs = FunctionFactory::instance().get("abs", context);
        auto plus = FunctionFactory::instance().get("plus", context);
        DataTypePtr res_type;
        for (size_t i = 0; i < tuple_size; ++i)
        {
            try
            {
                ColumnWithTypeAndName cur{cur_elements.empty() ? nullptr : cur_elements[i], cur_types[i], {}};
                auto elem_abs = abs->build(ColumnsWithTypeAndName{cur});

                if (i == 0)
                {
                    res_type = elem_abs->getResultType();
                    continue;
                }

                ColumnWithTypeAndName left_type{res_type, {}};
                ColumnWithTypeAndName right_type{elem_abs->getResultType(), {}};
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
        const auto * cur_tuple = checkAndGetDataType<DataTypeTuple>(arguments[0].type.get());
        const auto & cur_types = cur_tuple->getElements();
        auto cur_elements = getTupleElements(*arguments[0].column);

        size_t tuple_size = cur_elements.size();
        if (tuple_size == 0)
            return DataTypeUInt8().createColumnConstWithDefaultValue(input_rows_count);

        auto abs = FunctionFactory::instance().get("abs", context);
        auto plus = FunctionFactory::instance().get("plus", context);
        ColumnWithTypeAndName res;
        for (size_t i = 0; i < tuple_size; ++i)
        {
            ColumnWithTypeAndName cur{cur_elements[i], cur_types[i], {}};
            auto elem_abs = abs->build(ColumnsWithTypeAndName{cur});

            ColumnWithTypeAndName column;
            column.type = elem_abs->getResultType();
            column.column = elem_abs->execute({cur}, column.type, input_rows_count);

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
using FunctionL1Norm = FunctionLNorm<L1Label>;

template <>
class FunctionLNorm<L2SquaredLabel> : public ITupleFunction
{
public:
    static constexpr auto name = "l2_squared_norm";

    explicit FunctionLNorm(ContextPtr context_) : ITupleFunction(context_) {}
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionLNorm>(context_); }

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

        auto multiply = FunctionFactory::instance().get("multiply", context);
        auto plus = FunctionFactory::instance().get("plus", context);
        DataTypePtr res_type;
        for (size_t i = 0; i < tuple_size; ++i)
        {
            try
            {
                ColumnWithTypeAndName cur{cur_elements.empty() ? nullptr : cur_elements[i], cur_types[i], {}};
                auto elem_multiply = multiply->build(ColumnsWithTypeAndName{cur, cur});

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
        const auto * cur_tuple = checkAndGetDataType<DataTypeTuple>(arguments[0].type.get());
        const auto & cur_types = cur_tuple->getElements();
        auto cur_elements = getTupleElements(*arguments[0].column);

        size_t tuple_size = cur_elements.size();
        if (tuple_size == 0)
            return DataTypeUInt8().createColumnConstWithDefaultValue(input_rows_count);

        auto multiply = FunctionFactory::instance().get("multiply", context);
        auto plus = FunctionFactory::instance().get("plus", context);
        ColumnWithTypeAndName res;
        for (size_t i = 0; i < tuple_size; ++i)
        {
            ColumnWithTypeAndName cur{cur_elements[i], cur_types[i], {}};
            auto elem_multiply = multiply->build(ColumnsWithTypeAndName{cur, cur});

            ColumnWithTypeAndName column;
            column.type = elem_multiply->getResultType();
            column.column = elem_multiply->execute({cur, cur}, column.type, input_rows_count);

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
using FunctionL2SquaredNorm = FunctionLNorm<L2SquaredLabel>;

template <>
class FunctionLNorm<L2Label> : public FunctionL2SquaredNorm
{
private:
    using Base =  FunctionL2SquaredNorm;
public:
    static constexpr auto name = "l2_norm";

    explicit FunctionLNorm(ContextPtr context_) : Base(context_) {}
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionLNorm>(context_); }

    String getName() const override { return name; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        const auto * cur_tuple = checkAndGetDataType<DataTypeTuple>(arguments[0].type.get());

        if (!cur_tuple)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument 0 of function {} should be tuple, got {}",
                            getName(), arguments[0].type->getName());

        const auto & cur_types = cur_tuple->getElements();
        size_t tuple_size = cur_types.size();
        if (tuple_size == 0)
            return std::make_shared<DataTypeUInt8>();

        auto sqrt = FunctionFactory::instance().get("sqrt", context);
        return sqrt->build({ColumnWithTypeAndName{Base::getReturnTypeImpl(arguments), {}}})->getResultType();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto cur_elements = getTupleElements(*arguments[0].column);

        size_t tuple_size = cur_elements.size();
        if (tuple_size == 0)
            return DataTypeUInt8().createColumnConstWithDefaultValue(input_rows_count);

        ColumnWithTypeAndName squared_res;
        squared_res.type = Base::getReturnTypeImpl(arguments);
        squared_res.column = Base::executeImpl(arguments, squared_res.type, input_rows_count);

        auto sqrt = FunctionFactory::instance().get("sqrt", context);
        auto sqrt_elem = sqrt->build({squared_res});
        return sqrt_elem->execute({squared_res}, sqrt_elem->getResultType(), input_rows_count);
    }
};
using FunctionL2Norm = FunctionLNorm<L2Label>;

template <>
class FunctionLNorm<LinfLabel> : public ITupleFunction
{
public:
    static constexpr auto name = "linf_norm";

    explicit FunctionLNorm(ContextPtr context_) : ITupleFunction(context_) {}
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionLNorm>(context_); }

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

        auto abs = FunctionFactory::instance().get("abs", context);
        auto max = FunctionFactory::instance().get("max2", context);
        DataTypePtr res_type;
        for (size_t i = 0; i < tuple_size; ++i)
        {
            try
            {
                ColumnWithTypeAndName cur{cur_elements.empty() ? nullptr : cur_elements[i], cur_types[i], {}};
                auto elem_abs = abs->build(ColumnsWithTypeAndName{cur});

                if (i == 0)
                {
                    res_type = elem_abs->getResultType();
                    continue;
                }

                ColumnWithTypeAndName left_type{res_type, {}};
                ColumnWithTypeAndName right_type{elem_abs->getResultType(), {}};
                auto max_elem = max->build({left_type, right_type});
                res_type = max_elem->getResultType();
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
        const auto * cur_tuple = checkAndGetDataType<DataTypeTuple>(arguments[0].type.get());
        const auto & cur_types = cur_tuple->getElements();
        auto cur_elements = getTupleElements(*arguments[0].column);

        size_t tuple_size = cur_elements.size();
        if (tuple_size == 0)
            return DataTypeUInt8().createColumnConstWithDefaultValue(input_rows_count);

        auto abs = FunctionFactory::instance().get("abs", context);
        auto max = FunctionFactory::instance().get("max2", context);
        ColumnWithTypeAndName res;
        for (size_t i = 0; i < tuple_size; ++i)
        {
            ColumnWithTypeAndName cur{cur_elements[i], cur_types[i], {}};
            auto elem_abs = abs->build(ColumnsWithTypeAndName{cur});

            ColumnWithTypeAndName column;
            column.type = elem_abs->getResultType();
            column.column = elem_abs->execute({cur}, column.type, input_rows_count);

            if (i == 0)
            {
                res = std::move(column);
            }
            else
            {
                auto max_elem = max->build({res, column});
                auto res_type = max_elem->getResultType();
                res.column = max_elem->execute({res, column}, res_type, input_rows_count);
                res.type = res_type;
            }
        }

        return res.column;
    }
};
using FunctionLinfNorm = FunctionLNorm<LinfLabel>;

template <>
class FunctionLNorm<LpLabel> : public ITupleFunction
{
public:
    static constexpr auto name = "lp_norm";

    explicit FunctionLNorm(ContextPtr context_) : ITupleFunction(context_) {}
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionLNorm>(context_); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

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
        auto abs = FunctionFactory::instance().get("abs", context);
        auto pow = FunctionFactory::instance().get("pow", context);
        auto plus = FunctionFactory::instance().get("plus", context);
        DataTypePtr res_type;
        for (size_t i = 0; i < tuple_size; ++i)
        {
            try
            {
                ColumnWithTypeAndName cur{cur_elements.empty() ? nullptr : cur_elements[i], cur_types[i], {}};
                auto elem_abs = abs->build(ColumnsWithTypeAndName{cur});
                cur.type = elem_abs->getResultType();
                cur.column = cur.type->createColumn();

                auto elem_pow = pow->build(ColumnsWithTypeAndName{cur, p_column});

                if (i == 0)
                {
                    res_type = elem_pow->getResultType();
                    continue;
                }

                ColumnWithTypeAndName left_type{res_type, {}};
                ColumnWithTypeAndName right_type{elem_pow->getResultType(), {}};
                auto plus_elem = plus->build({left_type, right_type});
                res_type = plus_elem->getResultType();
            }
            catch (DB::Exception & e)
            {
                e.addMessage("While executing function {} for tuple element {}", getName(), i);
                throw;
            }
        }

        ColumnWithTypeAndName inv_p_column{std::make_shared<DataTypeFloat64>(), {}};
        return pow->build({ColumnWithTypeAndName{res_type, {}}, inv_p_column})->getResultType();
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

        if (!isColumnConst(*p_column.column) && p_column.column->size() != 1)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Second argument for function {} must be either constant float64 or constant uInt", getName());

        double p;
        if (isFloat(p_column.column->getDataType()))
            p = p_column.column->getFloat64(0);
        else if (isUInt(p_column.column->getDataType()))
            p = p_column.column->getUInt(0);
        else
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Second argument for function {} must be either constant float64 or constant uint", getName());

        if (p < 1 || p >= HUGE_VAL)
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                            "Second argument for function {} must be not less than one and not be an infinity",
                            getName());

        auto abs = FunctionFactory::instance().get("abs", context);
        auto pow = FunctionFactory::instance().get("pow", context);
        auto plus = FunctionFactory::instance().get("plus", context);
        ColumnWithTypeAndName res;
        for (size_t i = 0; i < tuple_size; ++i)
        {
            ColumnWithTypeAndName cur{cur_elements[i], cur_types[i], {}};
            auto elem_abs = abs->build(ColumnsWithTypeAndName{cur});
            cur.column = elem_abs->execute({cur}, elem_abs->getResultType(), input_rows_count);
            cur.type = elem_abs->getResultType();

            auto elem_pow = pow->build(ColumnsWithTypeAndName{cur, p_column});

            ColumnWithTypeAndName column;
            column.type = elem_pow->getResultType();
            column.column = elem_pow->execute({cur, p_column}, column.type, input_rows_count);

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

        ColumnWithTypeAndName inv_p_column{DataTypeFloat64().createColumnConst(input_rows_count, 1 / p),
                                           std::make_shared<DataTypeFloat64>(), {}};
        auto pow_elem = pow->build({res, inv_p_column});
        return pow_elem->execute({res, inv_p_column}, pow_elem->getResultType(), input_rows_count);
    }
};
using FunctionLpNorm = FunctionLNorm<LpLabel>;

template <class FuncLabel>
class FunctionLDistance : public ITupleFunction
{
public:
    /// constexpr cannot be used due to std::string has not constexpr constructor in this compiler version
    static inline auto name = std::string("l") + FuncLabel::name + "_distance";

    explicit FunctionLDistance(ContextPtr context_) : ITupleFunction(context_) {}
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionLDistance>(context_); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override
    {
        if constexpr (FuncLabel::name[0] == 'p')
            return 3;
        else
            return 2;
    }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override
    {
        if constexpr (FuncLabel::name[0] == 'p')
            return {2};
        else
            return {};
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        auto tuple_minus = FunctionFactory::instance().get("tuple_minus", context);
        ColumnsWithTypeAndName minus_args{arguments[0], arguments[1]};
        auto type = tuple_minus->build(minus_args)->getResultType();

        ColumnWithTypeAndName minus_res{type, {}};

        auto func = FunctionFactory::instance().get(std::string("l") + FuncLabel::name + "_norm", context);
        if constexpr (FuncLabel::name[0] == 'p')
            return func->build({minus_res, arguments[2]})->getResultType();
        else
            return func->build({minus_res})->getResultType();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto tuple_minus = FunctionFactory::instance().get("tuple_minus", context);
        ColumnsWithTypeAndName minus_args{arguments[0], arguments[1]};
        auto minus_func = tuple_minus->build(minus_args);
        auto type = minus_func->getResultType();
        auto column = minus_func->execute(minus_args, type, input_rows_count);

        ColumnWithTypeAndName minus_res{column, type, {}};

        auto func = FunctionFactory::instance().get(std::string("l") + FuncLabel::name + "_norm", context);
        if constexpr (FuncLabel::name[0] == 'p')
        {
            auto func_elem = func->build({minus_res, arguments[2]});
            return func_elem->execute({minus_res, arguments[2]}, func_elem->getResultType(), input_rows_count);
        }
        else
        {
            auto func_elem = func->build({minus_res});
            return func_elem->execute({minus_res}, func_elem->getResultType(), input_rows_count);
        }
    }
};

using FunctionL1Distance = FunctionLDistance<L1Label>;

using FunctionL2Distance = FunctionLDistance<L2Label>;

using FunctionL2SquaredDistance = FunctionLDistance<L2SquaredLabel>;

using FunctionLinfDistance = FunctionLDistance<LinfLabel>;

using FunctionLpDistance = FunctionLDistance<LpLabel>;

template <class FuncLabel>
class FunctionLNormalize : public ITupleFunction
{
public:
    /// constexpr cannot be used due to std::string has not constexpr constructor in this compiler version
    static inline auto name = std::string("l") + FuncLabel::name + "_normalize";

    explicit FunctionLNormalize(ContextPtr context_) : ITupleFunction(context_) {}
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionLNormalize>(context_); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override
    {
        if constexpr (FuncLabel::name[0] == 'p')
            return 2;
        else
            return 1;
    }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override
    {
        if constexpr (FuncLabel::name[0] == 'p')
            return {1};
        else
            return {};
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionLNorm<FuncLabel> norm(context);
        auto type = norm.getReturnTypeImpl(arguments);
        ColumnWithTypeAndName norm_res{type, {}};

        auto divide = FunctionFactory::instance().get("tuple_divide_by_number", context);
        return divide->build({arguments[0], norm_res})->getResultType();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        FunctionLNorm<FuncLabel> norm(context);
        auto type = norm.getReturnTypeImpl(arguments);
        auto column = norm.executeImpl(arguments, DataTypePtr(), input_rows_count);

        ColumnWithTypeAndName norm_res{column, type, {}};

        auto divide = FunctionFactory::instance().get("tuple_divide_by_number", context);
        ColumnsWithTypeAndName divide_args{arguments[0], norm_res};
        auto divide_elem = divide->build(divide_args);
        return divide_elem->execute(divide_args, divide_elem->getResultType(), input_rows_count);
    }
};

using FunctionL1Normalize = FunctionLNormalize<L1Label>;

using FunctionL2Normalize = FunctionLNormalize<L2Label>;

using FunctionLinfNormalize = FunctionLNormalize<LinfLabel>;

using FunctionLpNormalize = FunctionLNormalize<LpLabel>;

class FunctionCosineDistance : public ITupleFunction
{
public:
    /// constexpr cannot be used due to std::string has not constexpr constructor in this compiler version
    static inline auto name = "cosine_distance";

    explicit FunctionCosineDistance(ContextPtr context_) : ITupleFunction(context_) {}
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionCosineDistance>(context_); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        auto dot = FunctionFactory::instance().get("dot_product", context);
        ColumnWithTypeAndName dot_result{dot->build(arguments)->getResultType(), {}};

        FunctionL2Norm norm(context);
        ColumnWithTypeAndName first_norm{norm.getReturnTypeImpl({arguments[0]}), {}};
        ColumnWithTypeAndName second_norm{norm.getReturnTypeImpl({arguments[1]}), {}};

        auto minus = FunctionFactory::instance().get("minus", context);
        auto multiply = FunctionFactory::instance().get("multiply", context);
        auto divide = FunctionFactory::instance().get("divide", context);

        ColumnWithTypeAndName one{std::make_shared<DataTypeUInt8>(), {}};

        ColumnWithTypeAndName multiply_result{multiply->build({first_norm, second_norm})->getResultType(), {}};
        ColumnWithTypeAndName divide_result{divide->build({dot_result, multiply_result})->getResultType(), {}};
        return minus->build({one, divide_result})->getResultType();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        if (getReturnTypeImpl(arguments)->isNullable())
        {
            return DataTypeNullable(std::make_shared<DataTypeNothing>())
                   .createColumnConstWithDefaultValue(input_rows_count);
        }

        auto dot = FunctionFactory::instance().get("dot_product", context);
        auto dot_elem = dot->build(arguments);
        auto dot_type = dot_elem->getResultType();
        ColumnWithTypeAndName dot_result{dot_elem->execute(arguments, dot_type, input_rows_count),
                                         dot_type, {}};

        FunctionL2Norm norm(context);
        ColumnWithTypeAndName first_norm{norm.executeImpl({arguments[0]}, DataTypePtr(), input_rows_count),
                                         norm.getReturnTypeImpl({arguments[0]}), {}};
        ColumnWithTypeAndName second_norm{norm.executeImpl({arguments[1]}, DataTypePtr(), input_rows_count),
                                          norm.getReturnTypeImpl({arguments[1]}), {}};

        auto minus = FunctionFactory::instance().get("minus", context);
        auto multiply = FunctionFactory::instance().get("multiply", context);
        auto divide = FunctionFactory::instance().get("divide", context);

        ColumnWithTypeAndName one{DataTypeUInt8().createColumnConst(input_rows_count, 1),
                                  std::make_shared<DataTypeUInt8>(), {}};

        auto multiply_elem = multiply->build({first_norm, second_norm});
        ColumnWithTypeAndName multiply_result;
        multiply_result.type = multiply_elem->getResultType();
        multiply_result.column = multiply_elem->execute({first_norm, second_norm},
                                                        multiply_result.type, input_rows_count);

        auto divide_elem = divide->build({dot_result, multiply_result});
        ColumnWithTypeAndName divide_result;
        divide_result.type = divide_elem->getResultType();
        divide_result.column = divide_elem->execute({dot_result, multiply_result},
                                                    divide_result.type, input_rows_count);

        auto minus_elem = minus->build({one, divide_result});
        return minus_elem->execute({one, divide_result}, minus_elem->getResultType(), input_rows_count);
    }
};


/// An adaptor to call Norm/Distance function for tuple or array depending on the 1st argument type
template <class Traits>
class TupleOrArrayFunction : public IFunction
{
public:
    static constexpr auto name = Traits::name;

    explicit TupleOrArrayFunction(ContextPtr context_)
        : IFunction()
        , tuple_function(Traits::CreateTupleFunction(context_))
        , array_function(Traits::CreateArrayFunction(context_)) {}

    static FunctionPtr create(ContextPtr context_) { return std::make_shared<TupleOrArrayFunction>(context_); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return tuple_function->getNumberOfArguments(); }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        bool is_array = checkDataTypes<DataTypeArray>(arguments[0].type.get());
        return (is_array ? array_function : tuple_function)->getReturnTypeImpl(arguments);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        bool is_array = checkDataTypes<DataTypeArray>(arguments[0].type.get());
        return (is_array ? array_function : tuple_function)->executeImpl(arguments, result_type, input_rows_count);
    }

private:
    FunctionPtr tuple_function;
    FunctionPtr array_function;
};

extern FunctionPtr createFunctionArrayL1Norm(ContextPtr context_);
extern FunctionPtr createFunctionArrayL2Norm(ContextPtr context_);
extern FunctionPtr createFunctionArrayL2SquaredNorm(ContextPtr context_);
extern FunctionPtr createFunctionArrayLpNorm(ContextPtr context_);
extern FunctionPtr createFunctionArrayLinfNorm(ContextPtr context_);

extern FunctionPtr createFunctionArrayL1Distance(ContextPtr context_);
extern FunctionPtr createFunctionArrayL2Distance(ContextPtr context_);
extern FunctionPtr createFunctionArrayL2SquaredDistance(ContextPtr context_);
extern FunctionPtr createFunctionArrayLpDistance(ContextPtr context_);
extern FunctionPtr createFunctionArrayLinfDistance(ContextPtr context_);
extern FunctionPtr createFunctionArrayCosineDistance(ContextPtr context_);

struct L1NormTraits
{
    static constexpr auto name = "l1_norm";

    static constexpr auto CreateTupleFunction = FunctionL1Norm::create;
    static constexpr auto CreateArrayFunction = createFunctionArrayL1Norm;
};

struct L2NormTraits
{
    static constexpr auto name = "l2_norm";

    static constexpr auto CreateTupleFunction = FunctionL2Norm::create;
    static constexpr auto CreateArrayFunction = createFunctionArrayL2Norm;
};

struct L2SquaredNormTraits
{
    static constexpr auto name = "l2_squared_norm";

    static constexpr auto CreateTupleFunction = FunctionL2SquaredNorm::create;
    static constexpr auto CreateArrayFunction = createFunctionArrayL2SquaredNorm;
};

struct LpNormTraits
{
    static constexpr auto name = "lp_norm";

    static constexpr auto CreateTupleFunction = FunctionLpNorm::create;
    static constexpr auto CreateArrayFunction = createFunctionArrayLpNorm;
};

struct LinfNormTraits
{
    static constexpr auto name = "linf_norm";

    static constexpr auto CreateTupleFunction = FunctionLinfNorm::create;
    static constexpr auto CreateArrayFunction = createFunctionArrayLinfNorm;
};

struct L1DistanceTraits
{
    static constexpr auto name = "l1_distance";

    static constexpr auto CreateTupleFunction = FunctionL1Distance::create;
    static constexpr auto CreateArrayFunction = createFunctionArrayL1Distance;
};

struct L2DistanceTraits
{
    static constexpr auto name = "l2_distance";

    static constexpr auto CreateTupleFunction = FunctionL2Distance::create;
    static constexpr auto CreateArrayFunction = createFunctionArrayL2Distance;
};

struct L2SquaredDistanceTraits
{
    static constexpr auto name = "l2_squared_distance";

    static constexpr auto CreateTupleFunction = FunctionL2SquaredDistance::create;
    static constexpr auto CreateArrayFunction = createFunctionArrayL2SquaredDistance;
};

struct LpDistanceTraits
{
    static constexpr auto name = "lp_distance";

    static constexpr auto CreateTupleFunction = FunctionLpDistance::create;
    static constexpr auto CreateArrayFunction = createFunctionArrayLpDistance;
};

struct LinfDistanceTraits
{
    static constexpr auto name = "linf_distance";

    static constexpr auto CreateTupleFunction = FunctionLinfDistance::create;
    static constexpr auto CreateArrayFunction = createFunctionArrayLinfDistance;
};

struct CosineDistanceTraits
{
    static constexpr auto name = "cosine_distance";

    static constexpr auto CreateTupleFunction = FunctionCosineDistance::create;
    static constexpr auto CreateArrayFunction = createFunctionArrayCosineDistance;
};

using TupleOrArrayFunctionL1Norm = TupleOrArrayFunction<L1NormTraits>;
using TupleOrArrayFunctionL2Norm = TupleOrArrayFunction<L2NormTraits>;
using TupleOrArrayFunctionL2SquaredNorm = TupleOrArrayFunction<L2SquaredNormTraits>;
using TupleOrArrayFunctionLpNorm = TupleOrArrayFunction<LpNormTraits>;
using TupleOrArrayFunctionLinfNorm = TupleOrArrayFunction<LinfNormTraits>;

using TupleOrArrayFunctionL1Distance = TupleOrArrayFunction<L1DistanceTraits>;
using TupleOrArrayFunctionL2Distance = TupleOrArrayFunction<L2DistanceTraits>;
using TupleOrArrayFunctionL2SquaredDistance = TupleOrArrayFunction<L2SquaredDistanceTraits>;
using TupleOrArrayFunctionLpDistance = TupleOrArrayFunction<LpDistanceTraits>;
using TupleOrArrayFunctionLinfDistance = TupleOrArrayFunction<LinfDistanceTraits>;
using TupleOrArrayFunctionCosineDistance = TupleOrArrayFunction<CosineDistanceTraits>;

REGISTER_FUNCTION(VectorDistanceFunctions)
{
    factory.registerFunction<TupleOrArrayFunctionL1Norm>();
    factory.registerFunction<TupleOrArrayFunctionL2Norm>();
    factory.registerFunction<TupleOrArrayFunctionL2SquaredNorm>();
    factory.registerFunction<TupleOrArrayFunctionLinfNorm>();
    factory.registerFunction<TupleOrArrayFunctionLpNorm>();

    factory.registerAlias("norm_l1", TupleOrArrayFunctionL1Norm::name, FunctionFactory::CaseInsensitive);
    factory.registerAlias("norm_l2", TupleOrArrayFunctionL2Norm::name, FunctionFactory::CaseInsensitive);
    factory.registerAlias("norm_l2_squared", TupleOrArrayFunctionL2SquaredNorm::name, FunctionFactory::CaseInsensitive);
    factory.registerAlias("norm_linf", TupleOrArrayFunctionLinfNorm::name, FunctionFactory::CaseInsensitive);
    factory.registerAlias("norm_lp", FunctionLpNorm::name, FunctionFactory::CaseInsensitive);

    factory.registerFunction<TupleOrArrayFunctionL1Distance>();
    factory.registerFunction<TupleOrArrayFunctionL2Distance>();
    factory.registerFunction<TupleOrArrayFunctionL2SquaredDistance>();
    factory.registerFunction<TupleOrArrayFunctionLinfDistance>();
    factory.registerFunction<TupleOrArrayFunctionLpDistance>();

    factory.registerAlias("distance_l1", FunctionL1Distance::name, FunctionFactory::CaseInsensitive);
    factory.registerAlias("distance_l2", FunctionL2Distance::name, FunctionFactory::CaseInsensitive);
    factory.registerAlias("distance_l2_squared", FunctionL2SquaredDistance::name, FunctionFactory::CaseInsensitive);
    factory.registerAlias("distance_linf", FunctionLinfDistance::name, FunctionFactory::CaseInsensitive);
    factory.registerAlias("distance_lp", FunctionLpDistance::name, FunctionFactory::CaseInsensitive);

    factory.registerFunction<FunctionL1Normalize>();
    factory.registerFunction<FunctionL2Normalize>();
    factory.registerFunction<FunctionLinfNormalize>();
    factory.registerFunction<FunctionLpNormalize>();

    factory.registerAlias("normalize_l1", FunctionL1Normalize::name, FunctionFactory::CaseInsensitive);
    factory.registerAlias("normalize_l2", FunctionL2Normalize::name, FunctionFactory::CaseInsensitive);
    factory.registerAlias("normalize_linf", FunctionLinfNormalize::name, FunctionFactory::CaseInsensitive);
    factory.registerAlias("normalize_lp", FunctionLpNormalize::name, FunctionFactory::CaseInsensitive);

    factory.registerFunction<TupleOrArrayFunctionCosineDistance>();
}
}
