#include "config.h"

#if USE_PYTHON_UDF

#include <gtest/gtest.h>

#include <CPython/tests/CPythonTest.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/ISink.h>
#include <Processors/Port.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Transforms/PythonTableTransform.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>

using namespace DB;

namespace
{
class ValidatingRowCountSink final : public ISink
{
public:
    explicit ValidatingRowCountSink(Block header) : ISink(std::move(header), ProcessorID::EmptySinkID) { }

    String getName() const override { return "ValidatingRowCountSink"; }

    size_t getRowCount() const { return row_count; }

protected:
    void consume(Chunk chunk) override
    {
        const auto rows = chunk.getNumRows();
        /// Ensure header/columns match (this would throw on mismatched projections).
        (void)getPort().getHeader().cloneWithColumns(chunk.detachColumns());
        row_count += rows;
    }

private:
    size_t row_count = 0;
};
}

TEST_F(CPythonTest, PythonTableTransformPreservesRowCountWhenNoColumnsRequested)
{
    assertNoLeak([&]() {
        auto int32_type = std::make_shared<DataTypeInt32>();

        Block input_header = {ColumnWithTypeAndName{int32_type->createColumn(), int32_type, "i"}};

        auto col = ColumnInt32::create();
        col->insertValue(1);
        col->insertValue(2);
        col->insertValue(3);

        Columns columns;
        columns.emplace_back(std::move(col));
        Chunk input_chunk(std::move(columns), 3);

        auto source = std::make_shared<SourceFromSingleChunk>(input_header, std::move(input_chunk));

        Block output_header;
        ColumnsDescription output_columns{ColumnDescription{"value", int32_type}};

        const String python_source = R"PY(
def py_transform(i):
    return [(x,) for x in i]
)PY";

        auto transform = std::make_shared<PythonTableTransform>(
            source->getPort().getHeader(),
            output_header,
            python_source,
            "py_transform",
            Names{"i"},
            output_columns,
            Names{},
            PythonTableMode::Batch);

        auto sink = std::make_shared<ValidatingRowCountSink>(transform->getOutputPort().getHeader());

        connect(source->getPort(), transform->getInputPort());
        connect(transform->getOutputPort(), sink->getPort());

        auto processors = std::make_shared<Processors>();
        processors->emplace_back(source);
        processors->emplace_back(transform);
        processors->emplace_back(sink);

        QueryStatusPtr element;
        PipelineExecutor executor(processors, element);
        executor.execute(1);

        EXPECT_EQ(sink->getRowCount(), 3U);
    });
}

#endif
