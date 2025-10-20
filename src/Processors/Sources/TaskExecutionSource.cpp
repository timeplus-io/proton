#include <Processors/Sources/TaskExecutionSource.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsDateTime.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>


namespace DB
{

TaskExecutionSource::TaskExecutionSource(StorageID task_id, ContextPtr context_)
    : ISource(getHeader(), true, ProcessorID::TaskExecutionSourceID), task_execution(std::move(task_id)), context(std::move(context_))
{
}

Block TaskExecutionSource::getHeader()
{
    return Block{
        {std::make_shared<DataTypeDateTime64>(3), "start"},
        {std::make_shared<DataTypeDateTime64>(3), "end"},
        {std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()), "checkpoint"},
        {std::make_shared<DataTypeString>(), "result"},
    };
}

Chunk TaskExecutionSource::generate()
{
    if (executed || is_cancelled)
        return {};

    /// Execute the task
    auto result = task_execution.execute(context);
    executed = true;

    /// start, end, checkpoint, result
    auto start_col = ColumnDateTime64::create(0, 3);
    auto end_col = ColumnDateTime64::create(0, 3);
    auto checkpoint_key_col = ColumnString::create();
    auto checkpoint_val_col = ColumnString::create();
    auto checkpoint_offset_col = ColumnArray::ColumnOffsets::create();
    auto result_col = ColumnString::create();

    /// Provision block from task execution result
    start_col->insert(DateTime64(result.execution_start));
    end_col->insert(DateTime64(result.execution_end));
    for (const auto & [k, v] : result.checkpoint)
    {
        checkpoint_key_col->insert(k);
        checkpoint_val_col->insert(v);
    }
    checkpoint_offset_col->insert(checkpoint_key_col->size());
    result_col->insert(result.displayError());

    return Chunk{
        Columns{
            std::move(start_col),
            std::move(end_col),
            ColumnMap::create(
                ColumnPtr(std::move(checkpoint_key_col)),
                ColumnPtr(std::move(checkpoint_val_col)),
                ColumnPtr(std::move(checkpoint_offset_col))),
            std::move(result_col),
        },
        /*num_rows_=*/1};
}
}
