#pragma once

#include <Core/Streaming/WatermarkInfo.h>
#include <Functions/IFunction.h>
#include <Common/Streaming/Substream/Executor.h>

#include <algorithm>
#include <limits>

namespace DB
{
namespace Streaming::Substream
{

template <typename T>
concept SimpleFunctionPtr = requires(T && func, const ColumnsWithTypeAndName & arguments, DataTypePtr result_type, size_t rows)
{
    {
        func->execute(arguments, result_type, rows)
        } -> std::same_as<ColumnPtr>;
};

template <typename T>
concept SimpleTemplateFunctionPtr = requires(T && template_function, const ColumnsWithTypeAndName & template_arguments)
{
    {
        template_function->build(template_arguments)
        } -> SimpleFunctionPtr;
};

/// Function Executor For Substream
template <SimpleTemplateFunctionPtr TemplateFunction = std::shared_ptr<IFunctionOverloadResolver>>
struct SimpleFunctionWorkspace
{
    TemplateFunction template_function;
    ColumnsWithTypeAndName template_arguments; /// maybe has const columns.
    std::vector<size_t> argument_column_indices;
    DataTypePtr result_type;
};

template <
    SimpleTemplateFunctionPtr TemplateFunction = std::shared_ptr<IFunctionOverloadResolver>,
    SimpleFunctionPtr Function = std::shared_ptr<IFunctionBase>>
class SimpleFunctionExecutor final : public Executor<SimpleFunctionExecutor<TemplateFunction, Function>, std::vector<Function>>
{
public:
    using Executor = Executor<SimpleFunctionExecutor<TemplateFunction, Function>, std::vector<Function>>;
    using DataWithID = typename Executor::DataWithID;
    using DataWithIDRows = typename Executor::DataWithIDRows;

    struct Workspace
    {
        TemplateFunction template_function;
        ColumnsWithTypeAndName template_arguments; /// maybe has const columns.
        std::vector<size_t> argument_column_indices;
        DataTypePtr result_type;
    };

private:
    size_t input_num_columns = 0;

    std::vector<Workspace> insts;
    /// result contains extra columns of indices
    std::vector<size_t> context_column_indices;

    Columns empty_output_columns;

public:
    SimpleFunctionExecutor(
        GroupByKeys groupby_keys_type_,
        Block header_,
        std::vector<size_t> keys_indices_,
        std::vector<Workspace> insts_,
        std::vector<size_t> context_column_indices_ = {})
        : Executor(std::move(header_), std::move(keys_indices_), groupby_keys_type_)
        , input_num_columns(Executor::table().header.columns())
        , insts(std::move(insts_))
        , context_column_indices(std::move(context_column_indices_))
        , empty_output_columns(getEmptyOutputColumns())
    {
    }

    void createSubstream(const ID &, char * place)
    {
        auto * data = new (place) std::vector<Function>(insts.size());
        for (size_t i = 0; const auto & inst : insts)
            (*data)[i++] = inst.template_function->build(inst.template_arguments);
    }

    /// Excute functions for input columns and return result columns of functions
    /// Parmeters:
    ///   @columns:                   input columns
    ///   @rows:                      input columns rows
    /// Return:
    ///   [context columns] + result columns
    Columns execute(const Columns & columns, size_t rows)
    {
        if (insts.empty() || rows == 0)
            return empty_output_columns;

        size_t results_size = empty_output_columns.size();
        Columns results(results_size);
        size_t result_idx = 0;

        Executor::execute(
            columns, rows, [&](std::vector<DataWithIDRows> & datas, const IColumn::Selector & selector, const Columns & cols) {
                size_t num_substreams = datas.size();
                assert(num_substreams != 0);

                /// We want to avoid duplicate splits of the same argument column for multiple functions and no needed columns
                /// Init input columns and result columns for each substream
                /// Substream-1 -> Input columns or Result columns
                /// Substream-2 -> Input columns or Result columns
                /// ...
                /// Substream-n -> Input columns or Result columns
                std::vector<Columns> input_columns_to_substreams(num_substreams, Columns(input_num_columns));
                std::vector<Columns> result_columns_to_substreams(num_substreams, Columns(insts.size()));

                /// For each functions
                for (size_t inst_idx = 0; auto & inst : insts)
                {
                    /// Prepare arguments per substream
                    std::vector<ColumnsWithTypeAndName> sub_arguments_vec(num_substreams, inst.template_arguments);
                    for (size_t col_idx = 0; auto arg_idx : inst.argument_column_indices)
                    {
                        populateSubstreamColumnsIfNeed(input_columns_to_substreams, arg_idx, num_substreams, selector, cols);
                        for (size_t sub_idx = 0; auto & sub_columns : input_columns_to_substreams)
                            sub_arguments_vec[sub_idx++][col_idx].column = sub_columns[arg_idx];
                        ++col_idx;
                    }

                    /// Calc result per substream
                    for (size_t sub_idx = 0; auto & [sub_data, _, sub_rows] : datas)
                    {
                        const auto & curr_sub_arguments = sub_arguments_vec[sub_idx];
                        result_columns_to_substreams[sub_idx][inst_idx]
                            = sub_data[inst_idx]->execute(curr_sub_arguments, inst.result_type, sub_rows);
                        ++sub_idx;
                    }
                    ++inst_idx;
                }

                /// Results: function [context columns] + result columns
                /// Merge result substream columns
                /// Populate and merge requried non-result substream columns
                for (auto col_idx : context_column_indices)
                {
                    populateSubstreamColumnsIfNeed(input_columns_to_substreams, col_idx, num_substreams, selector, cols);
                    results[result_idx] = mergeSubstreamColumns(input_columns_to_substreams, col_idx);
                    assert(results[result_idx]->size() == rows);
                    ++result_idx;
                }

                /// Merge functions substream results
                size_t insts_size = insts.size();
                for (size_t inst_idx = 0; inst_idx < insts_size; ++inst_idx)
                {
                    results[result_idx] = mergeSubstreamColumns(result_columns_to_substreams, inst_idx);
                    assert(results[result_idx]->size() == rows);
                    ++result_idx;
                }
            });

        return results;
    }

private:
    void populateSubstreamColumnsIfNeed(
        std::vector<Columns> & columns_to_substreams,
        size_t col_idx,
        size_t num_substreams,
        const IColumn::Selector & selector,
        const Columns & input_columns)
    {
        assert(columns_to_substreams.size() == num_substreams);
        assert(num_substreams != 0 && col_idx < columns_to_substreams[0].size());
        auto & curr_substream_column = columns_to_substreams[0][col_idx];
        if (curr_substream_column)
            return; /// populated

        assert(col_idx < input_columns.size());
        auto substream_cols = input_columns[col_idx]->scatter(num_substreams, selector);
        assert(substream_cols.size() == num_substreams);
        for (size_t sub_idx = 0; auto & sub_col : substream_cols)
            columns_to_substreams[sub_idx++][col_idx] = std::move(sub_col);
    }

    ColumnPtr mergeSubstreamColumns(const std::vector<Columns> & columns_to_substreams, size_t col_idx)
    {
        size_t num_substreams = columns_to_substreams.size();
        assert(num_substreams != 0 && col_idx < columns_to_substreams[0].size());

        /// Avoid merge and copy first substream per column.
        auto merged_column = IColumn::mutate(columns_to_substreams[0][col_idx]);
        for (size_t sub_idx = 1; sub_idx < num_substreams; ++sub_idx)
        {
            auto & curr_substream_column = *columns_to_substreams[sub_idx][col_idx];
            merged_column->insertRangeFrom(curr_substream_column, 0, curr_substream_column.size());
        }

        return std::move(merged_column);
    }

    Columns getEmptyOutputColumns()
    {
        Columns cols;
        cols.reserve(context_column_indices.size() + insts.size());
        for (auto idx : context_column_indices)
            cols.emplace_back(Executor::table().header.safeGetByPosition(idx).type->createColumn());
        for (const auto & inst : insts)
            cols.emplace_back(inst.result_type->createColumn());

        return cols;
    }
};

}
}
