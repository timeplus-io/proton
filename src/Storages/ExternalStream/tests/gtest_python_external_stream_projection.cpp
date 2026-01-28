#include "config.h"

#if USE_PYTHON_UDF

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <CPython/tests/CPythonTest.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/ISink.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/ExternalStream/Python/StoragePythonTable.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageSnapshot.h>

#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{
class RowCountSink final : public ISink
{
public:
    explicit RowCountSink(Block header) : ISink(std::move(header), ProcessorID::EmptySinkID) { }

    String getName() const override { return "RowCountSink"; }

    size_t getRowCount() const { return row_count; }
    size_t getColumnCount() const { return column_count; }

protected:
    void consume(Chunk chunk) override
    {
        row_count += chunk.getNumRows();
        column_count = chunk.getColumns().size();
    }

private:
    size_t row_count = 0;
    size_t column_count = 0;
};
}

TEST_F(CPythonTest, PythonExternalStreamPreservesRowCountWhenNoColumnsProjected)
{
    tryRegisterFormats();

    auto context = Context::createCopy(getContext().context);

    ColumnsDescription columns{
        {"n", std::make_shared<DataTypeInt64>()},
        {"s", std::make_shared<DataTypeString>()},
    };

    const std::string python_source = R"PY(
def emit_rows():
    return [
        (1, "a"),
        (2, "b"),
        (3, "c"),
    ]
)PY";

    auto storage = StoragePythonTable::create(
        StorageID("default", "gtest_python_ext"),
        columns,
        "emit_rows",
        python_source,
        PythonTableMode::Batch,
        "" /* sink_function_name */);

    storage->startup();
    auto snapshot = std::make_shared<StorageSnapshot>(*storage, storage->getInMemoryMetadataPtr());

    SelectQueryInfo query_info;
    Names column_names; // empty projection

    QueryPlan query_plan;
    storage->read(
        query_plan,
        column_names,
        snapshot,
        query_info,
        context,
        QueryProcessingStage::FetchColumns,
        context->getSettingsRef().max_block_size.value,
        1);

    auto pipeline_builder = query_plan.buildQueryPipeline(
        QueryPlanOptimizationSettings::fromContext(context),
        BuildQueryPipelineSettings::fromContext(context),
        context);

    QueryPlanResourceHolder resources;
    Pipe pipe = QueryPipelineBuilder::getPipe(std::move(*pipeline_builder), resources);

    auto header = pipe.getHeader();
    auto * output = pipe.getOutputPort(0);
    auto processors = Pipe::detachProcessors(std::move(pipe));

    auto sink = std::make_shared<RowCountSink>(header);
    connect(*output, sink->getPort());
    processors.emplace_back(sink);

    QueryStatusPtr element;
    auto pipeline = std::make_shared<Processors>(std::move(processors));
    PipelineExecutor executor(pipeline, element);
    executor.execute(1);

    EXPECT_EQ(sink->getColumnCount(), 0U);
    EXPECT_EQ(sink->getRowCount(), 3U);
}

#endif
