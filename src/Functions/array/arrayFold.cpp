#include "FunctionArrayMapped.h"
#include <Functions/FunctionFactory.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int SIZES_OF_ARRAYS_DOESNT_MATCH;
    extern const int TYPE_MISMATCH;
}

/**
 * arrayFold(x1,...,xn,accum -> expression, array1,...,arrayn, accum_initial) - apply the expression to each element of the array (or set of arrays).
 */
class ArrayFold : public IFunction
{
public:
    static constexpr auto name = "arrayFold";
    static FunctionPtr create(ContextPtr) { return std::make_shared<ArrayFold>(); }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    /// Avoid the default adaptors since they modify the inputs and that makes knowing the lambda argument types
    /// (getLambdaArgumentTypes) more complex, as it requires knowing what the adaptors will do
    /// It's much simpler to avoid the adapters
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    void getLambdaArgumentTypes(DataTypes & arguments) const override
    {
        if (arguments.size() < 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires as arguments a lambda function, at least one array and an accumulator argument", getName());

        DataTypes nested_types(arguments.size() - 1);
        for (size_t i = 0; i < nested_types.size() - 1; ++i)
        {
            const auto * array_type = checkAndGetDataType<DataTypeArray>(&*arguments[i + 1]);
            if (!array_type)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument {} of function {} must be array, found {} instead", i + 2, getName(), arguments[i + 1]->getName());
            nested_types[i] = recursiveRemoveLowCardinality(array_type->getNestedType());
        }
        nested_types[nested_types.size() - 1] = arguments[arguments.size() - 1];

        const auto * function_type = checkAndGetDataType<DataTypeFunction>(arguments[0].get());
        if (!function_type || function_type->getArgumentTypes().size() != nested_types.size())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument for this overload of {} must be a function with {} arguments, found {} instead.",
                            getName(), nested_types.size(), arguments[0]->getName());

        arguments[0] = std::make_shared<DataTypeFunction>(nested_types);
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() < 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires at least 2 arguments, passed: {}.", getName(), arguments.size());

        const auto * data_type_function = checkAndGetDataType<DataTypeFunction>(arguments[0].type.get());
        if (!data_type_function)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument for function {} must be a function", getName());

        auto accumulator_type = arguments.back().type;
        auto lambda_type = data_type_function->getReturnType();
        if (!accumulator_type->equals(*lambda_type))
            throw Exception(ErrorCodes::TYPE_MISMATCH,
                    "Return type of lambda function must be the same as the accumulator type, inferred return type of lambda: {}, inferred type of accumulator: {}",
                    lambda_type->getName(), accumulator_type->getName());

        return accumulator_type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto & lambda_with_type_and_name = arguments[0];

        if (!lambda_with_type_and_name.column)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument for function {} must be a function", getName());

        const auto * lambda_function = typeid_cast<const ColumnFunction *>(lambda_with_type_and_name.column.get());
        if (!lambda_function)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument for function {} must be a function", getName());

        ColumnPtr offsets_column;
        ColumnPtr column_first_array_ptr;
        const ColumnArray * column_first_array = nullptr;
        ColumnsWithTypeAndName arrays;
        arrays.reserve(arguments.size() - 1);
        /// Validate input types and get input array columns in convenient form
        for (size_t i = 1; i < arguments.size() - 1; ++i)
        {
            const auto & array_with_type_and_name = arguments[i];
            ColumnPtr column_array_ptr = array_with_type_and_name.column;
            const auto * column_array = checkAndGetColumn<ColumnArray>(column_array_ptr.get());
            if (!column_array)
            {
                const ColumnConst * column_const_array = checkAndGetColumnConst<ColumnArray>(column_array_ptr.get());
                if (!column_const_array)
                    throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Expected array column, found {}", column_array_ptr->getName());
                column_array_ptr = recursiveRemoveLowCardinality(column_const_array->convertToFullColumn());
                column_array = checkAndGetColumn<ColumnArray>(column_array_ptr.get());
            }

            const DataTypePtr & array_type_ptr = array_with_type_and_name.type;
            const auto * array_type = checkAndGetDataType<DataTypeArray>(array_type_ptr.get());
            if (!array_type)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Expected array type, found {}", array_type_ptr->getName());

            if (!offsets_column)
                offsets_column = column_array->getOffsetsPtr();
            else
            {
                /// The first condition is optimization: do not compare data if the pointers are equal.
                if (column_array->getOffsetsPtr() != offsets_column
                    && column_array->getOffsets() != typeid_cast<const ColumnArray::ColumnOffsets &>(*offsets_column).getData())
                    throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH, "Arrays passed to {} must have equal size", getName());
            }
            if (i == 1)
            {
                column_first_array_ptr = column_array_ptr;
                column_first_array = column_array;
            }
            arrays.emplace_back(ColumnWithTypeAndName(column_array->getDataPtr(),
                                                      recursiveRemoveLowCardinality(array_type->getNestedType()),
                                                      array_with_type_and_name.name));
        }

        ssize_t rows_count = input_rows_count;
        ssize_t data_row_count = arrays[0].column->size();
        size_t array_count = arrays.size();

        if (rows_count == 0)
            return arguments.back().column->convertToFullColumnIfConst()->cloneEmpty();

        ColumnPtr current_column;
        current_column = arguments.back().column->convertToFullColumnIfConst();
        MutableColumnPtr result_data = arguments.back().column->convertToFullColumnIfConst()->cloneEmpty();

        size_t max_array_size = 0;
        const auto & offsets = column_first_array->getOffsets();

        IColumn::Selector selector(data_row_count);
        size_t cur_ind = 0;
        ssize_t cur_arr = 0;

        /// skip to the first non empty array
        if (data_row_count)
            while (offsets[cur_arr] == 0)
                ++cur_arr;

        /// selector[i] is an index that i_th data element has in an array it corresponds to
        for (ssize_t i = 0; i < data_row_count; ++i)
        {
            selector[i] = cur_ind;
            cur_ind++;
            if (cur_ind > max_array_size)
                max_array_size = cur_ind;
            while (cur_arr < rows_count && cur_ind >= offsets[cur_arr] - offsets[cur_arr - 1])
            {
                ++cur_arr;
                cur_ind = 0;
            }
        }

        std::vector<MutableColumns> data_arrays;
        data_arrays.resize(array_count);

        /// Split each data column to columns containing elements of only Nth index in array
        if (max_array_size > 0)
            for (size_t i = 0; i < array_count; ++i)
                data_arrays[i] = arrays[i].column->scatter(max_array_size, selector);

        ColumnPtr accumulator_col = arguments.back().column->convertToFullColumnIfConst();
        MutableColumnPtr result_col = accumulator_col->cloneEmpty();
        ColumnPtr lambda_col = lambda_function->cloneResized(num_rows);

        IColumn::Permutation inverse_permutation(rows_count);
        size_t inverse_permutation_count = 0;

        /// current_column after each iteration contains value of accumulator after applying values under indexes of arrays.
        /// At each iteration only rows of current_column with arrays that still has unapplied elements are kept.
        /// Discarded rows which contain finished calculations are added to result_data column and as we insert them we save their original row_number in inverse_permutation vector
        for (size_t ind = 0; ind < max_array_size; ++ind)
        {
            IColumn::Selector prev_selector(prev_size);
            size_t prev_ind = 0;
            for (ssize_t irow = 0; irow < rows_count; ++irow)
            {
                if (offsets[irow] - offsets[irow - 1] > ind)
                    prev_selector[prev_ind++] = 1;
                else if (offsets[irow] - offsets[irow - 1] == ind)
                {
                    inverse_permutation[inverse_permutation_count++] = irow;
                    prev_selector[prev_ind++] = 0;
                }
            }
            auto prev = current_column->scatter(2, prev_selector);

            /// Scatter the accumulator into two columns
            /// - one column with accumulator values for rows less than slice-many elements, no further calculation is performed on them
            /// - one column with accumulator values for rows with slice-many or more elements, these are updated in this or following iteration
            std::vector<IColumn::MutablePtr> finished_unfinished_accumulator_values = accumulator_col->scatter(2, prev_selector);
            IColumn::MutablePtr & finished_accumulator_values = finished_unfinished_accumulator_values[0];
            IColumn::MutablePtr & unfinished_accumulator_values = finished_unfinished_accumulator_values[1];

            /// Copy finished accumulator values into the result
            result_col->insertRangeFrom(*finished_accumulator_values, 0, finished_accumulator_values->size());

            /// The lambda function can contain statically bound arguments, in particular their row values. We need to filter for the rows
            /// we care about.
            IColumn::Filter filter(unfinished_rows);
            for (size_t i = 0; i < prev_selector.size(); ++i)
                filter[i] = prev_selector[i];
            ColumnPtr lambda_col_filtered = lambda_col->filter(filter, lambda_col->size());
            IColumn::MutablePtr lambda_col_filtered_cloned = lambda_col_filtered->cloneResized(lambda_col_filtered->size()); /// clone so we can bind more arguments
            auto * lambda = typeid_cast<ColumnFunction *>(lambda_col_filtered_cloned.get());

            /// Bind arguments to lambda function (accumulator + array arguments)
            lambda->appendArguments(std::vector({ColumnWithTypeAndName(std::move(unfinished_accumulator_values), arguments.back().type, arguments.back().name)}));
            for (size_t array_col = 0; array_col < num_array_cols; ++array_col)
                lambda->appendArguments(std::vector({ColumnWithTypeAndName(std::move(vertical_slices[array_col][slice]), arrays_data_with_type_and_name[array_col].type, arrays_data_with_type_and_name[array_col].name)}));

            /// Perform the actual calculation and copy the result into the accumulator
            ColumnWithTypeAndName res_with_type_and_name = lambda->reduce();
            accumulator_col = res_with_type_and_name.column->convertToFullColumnIfConst();

            unfinished_rows = accumulator_col->size();
            lambda_col = lambda_col_filtered;
        }

        result_data->insertRangeFrom(*current_column, 0, current_column->size());
        for (ssize_t irow = 0; irow < rows_count; ++irow)
            if (offsets[irow] - offsets[irow - 1] == max_array_size)
                inverse_permutation[inverse_permutation_count++] = irow;

        /// We have result_data containing result for every row and inverse_permutation which contains indexes of rows in input it corresponds to.
        /// Now we need to invert inverse_permuation and apply it to result_data to get rows in right order.
        IColumn::Permutation perm(rows_count);
        for (ssize_t i = 0; i < rows_count; i++)
            perm[inverse_permutation[i]] = i;
        return result_data->permute(perm, 0);
    }

private:
    String getName() const override
    {
        return name;
    }
};

REGISTER_FUNCTION(ArrayFold)
{
    factory.registerFunction<ArrayFold>();
}
}
