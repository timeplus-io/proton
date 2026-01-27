#include "config.h"

#if USE_PYTHON_UDF

#include <gtest/gtest.h>

#include <CPython/GILGuard.h>
#include <CPython/PyObjectPtr.h>
#include <CPython/tests/CPythonTest.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/ISink.h>
#include <Processors/Sources/PythonStreamingSource.h>
#include <Common/assert_cast.h>

#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>

#include <datetime.h>

using namespace DB;

namespace
{
class CollectBlocksSink final : public ISink
{
public:
    explicit CollectBlocksSink(Block header) : ISink(std::move(header), ProcessorID::EmptySinkID) { }

    String getName() const override { return "CollectBlocksSink"; }

    const std::vector<Block> & getBlocks() const { return blocks; }

protected:
    void consume(Chunk chunk) override { blocks.emplace_back(getPort().getHeader().cloneWithColumns(chunk.detachColumns())); }

private:
    std::vector<Block> blocks;
};
}

TEST_F(CPythonTest, PythonStreamingSourceProjectionRespectsHeader)
{
    PyDateTime_IMPORT;

    assertNoLeak([&]() {
        auto string_type = std::make_shared<DataTypeString>();
        auto datetime64_type = std::make_shared<DataTypeDateTime64>(3);

        DataTypes element_types = {string_type, string_type, string_type, string_type, datetime64_type};
        Strings element_names = {"type", "product_id", "channel", "full_payload", "received_at"};
        auto tuple_type = std::make_shared<DataTypeTuple>(element_types, element_names);

        Block header = {ColumnWithTypeAndName{string_type->createColumn(), string_type, "type"}};

        cpython::PyObjectPtr iterator;
        {
            cpython::GILGuard gil_guard(/*use_need_cleanup=*/true);

            cpython::PyObjectPtr rows{PyList_New(1)};
            ASSERT_TRUE(rows);

            PyObject * row = PyTuple_New(5);
            ASSERT_TRUE(row);
            PyTuple_SET_ITEM(row, 0, PyUnicode_FromString("ticker"));
            PyTuple_SET_ITEM(row, 1, PyUnicode_FromString("BTC-USD"));
            PyTuple_SET_ITEM(row, 2, PyUnicode_FromString(""));
            PyTuple_SET_ITEM(row, 3, PyUnicode_FromString("{\"type\":\"ticker\"}"));
            PyTuple_SET_ITEM(row, 4, PyDateTime_FromDateAndTime(2026, 1, 22, 1, 53, 19, 640490));

            PyList_SET_ITEM(rows.get(), 0, row);

            iterator = cpython::PyObjectPtr{PyObject_GetIter(rows.get())};
            ASSERT_TRUE(iterator);
        }

        auto source = std::make_shared<PythonStreamingSource>(header, std::move(iterator), tuple_type, "" /* module_name */);
        auto sink = std::make_shared<CollectBlocksSink>(source->getPort().getHeader());

        connect(source->getPort(), sink->getPort());

        auto processors = std::make_shared<Processors>();
        processors->emplace_back(source);
        processors->emplace_back(sink);

        QueryStatusPtr element;
        PipelineExecutor executor(processors, element);
        executor.execute(1);

        ASSERT_EQ(sink->getBlocks().size(), 1U);
        const auto & block = sink->getBlocks().front();
        ASSERT_EQ(block.columns(), 1U);
        ASSERT_TRUE(block.has("type"));

        const auto & col = assert_cast<const ColumnString &>(*block.getByName("type").column);
        ASSERT_EQ(col.getDataAt(0).toString(), "ticker");
    });
}

TEST_F(CPythonTest, PythonStreamingSourceProjectionSkipsUnselectedConversion)
{
    assertNoLeak([&]() {
        auto string_type = std::make_shared<DataTypeString>();
        auto int32_type = std::make_shared<DataTypeInt32>();

        DataTypes element_types = {string_type, int32_type};
        Strings element_names = {"type", "bad_int"};
        auto tuple_type = std::make_shared<DataTypeTuple>(element_types, element_names);

        Block header = {ColumnWithTypeAndName{string_type->createColumn(), string_type, "type"}};

        cpython::PyObjectPtr iterator;
        {
            cpython::GILGuard gil_guard(/*use_need_cleanup=*/true);

            cpython::PyObjectPtr rows{PyList_New(1)};
            ASSERT_TRUE(rows);

            PyObject * row = PyTuple_New(2);
            ASSERT_TRUE(row);
            PyTuple_SET_ITEM(row, 0, PyUnicode_FromString("ticker"));
            /// This value cannot be converted to Int32, but should not matter for projection.
            PyTuple_SET_ITEM(row, 1, PyUnicode_FromString("not_an_int"));

            PyList_SET_ITEM(rows.get(), 0, row);

            iterator = cpython::PyObjectPtr{PyObject_GetIter(rows.get())};
            ASSERT_TRUE(iterator);
        }

        auto source = std::make_shared<PythonStreamingSource>(header, std::move(iterator), tuple_type, "" /* module_name */);
        auto sink = std::make_shared<CollectBlocksSink>(source->getPort().getHeader());

        connect(source->getPort(), sink->getPort());

        auto processors = std::make_shared<Processors>();
        processors->emplace_back(source);
        processors->emplace_back(sink);

        QueryStatusPtr element;
        PipelineExecutor executor(processors, element);
        executor.execute(1);

        ASSERT_EQ(sink->getBlocks().size(), 1U);
        const auto & block = sink->getBlocks().front();
        ASSERT_EQ(block.columns(), 1U);
        ASSERT_TRUE(block.has("type"));

        const auto & col = assert_cast<const ColumnString &>(*block.getByName("type").column);
        ASSERT_EQ(col.getDataAt(0).toString(), "ticker");
    });
}

#endif
