#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>

namespace DB::Streaming
{

struct AggregateFunctionInstruction
{
    const IAggregateFunction * that{};
    size_t state_offset{};
    const IColumn ** arguments{};
    const IAggregateFunction * batch_that{};
    const IColumn ** batch_arguments{};
    const IColumn * delta_column{};
    const UInt64 * offsets{};
    bool has_sparse_arguments = false;
};

/// This array serves two purposes.
/// Function arguments are collected side by side, and they do not need to be collected from different places. Also the array is made zero-terminated.
/// The inner loop (for the case without_key) is almost twice as compact; performance gain of about 30%.
using AggregateFunctionInstructions = std::vector<AggregateFunctionInstruction>;
using NestedColumnsHolder = std::vector<std::vector<const IColumn *>>;

/// Aliases
using AggregateColumns = std::vector<ColumnRawPtrs>;
using AggregateColumnsData = std::vector<ColumnAggregateFunction::Container *>;
using AggregateColumnsConstData = std::vector<const ColumnAggregateFunction::Container *>;
using AggregateFunctionsPlainPtrs = std::vector<const IAggregateFunction *>;


}
