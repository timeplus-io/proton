#include "config.h"

#if USE_PYTHON_UDF

#include <gtest/gtest.h>

#include <CPython/GILGuard.h>
#include <CPython/tests/CPythonTest.h>
#include <Common/Exception.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/ISink.h>
#include <Processors/Port.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Transforms/PythonTableTransform.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>

#include <base/scope_guard.h>

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

using namespace DB;

namespace DB::ErrorCodes
{
extern const int QUERY_WAS_CANCELLED;
}

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

class CollectingBlocksSink final : public ISink
{
public:
    explicit CollectingBlocksSink(Block header) : ISink(std::move(header), ProcessorID::EmptySinkID) { }

    String getName() const override { return "CollectingBlocksSink"; }

    const std::vector<Block> & getBlocks() const { return blocks; }

protected:
    void consume(Chunk chunk) override
    {
        blocks.emplace_back(getPort().getHeader().cloneWithColumns(chunk.detachColumns()));
    }

private:
    std::vector<Block> blocks;
};

bool waitForPythonFlag(const char * attr_name, std::chrono::milliseconds timeout)
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline)
    {
        {
            cpython::GILGuard gil_guard(/*use_need_cleanup=*/true);
            cpython::PyObjectPtr builtins{PyImport_ImportModule("builtins")};
            if (builtins)
            {
                cpython::PyObjectPtr flag{PyObject_GetAttrString(builtins.get(), attr_name)};
                if (flag)
                {
                    const int is_true = PyObject_IsTrue(flag.get());
                    if (is_true == 1)
                        return true;
                    if (is_true < 0)
                        PyErr_Clear();
                }
                else
                {
                    PyErr_Clear();
                }
            }
            else
            {
                PyErr_Clear();
            }
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    return false;
}

void clearPythonFlag(const char * attr_name)
{
    cpython::GILGuard gil_guard(/*use_need_cleanup=*/true);
    cpython::PyObjectPtr builtins{PyImport_ImportModule("builtins")};
    if (!builtins)
    {
        PyErr_Clear();
        return;
    }

    if (PyObject_DelAttrString(builtins.get(), attr_name) < 0)
        PyErr_Clear();
}
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

TEST_F(CPythonTest, PythonTableTransformCancelDuringGeneratorIterationDoesNotEmitPartialChunk)
{
    assertNoLeak([&]() {
        constexpr auto blocked_flag = "_tp_python_table_transform_blocked";

        clearPythonFlag(blocked_flag);
        SCOPE_EXIT({ clearPythonFlag(blocked_flag); });

        auto int32_type = std::make_shared<DataTypeInt32>();

        Block input_header = {ColumnWithTypeAndName{int32_type->createColumn(), int32_type, "i"}};

        auto col = ColumnInt32::create();
        col->insertValue(1);

        Columns columns;
        columns.emplace_back(std::move(col));
        Chunk input_chunk(std::move(columns), 1);

        auto source = std::make_shared<SourceFromSingleChunk>(input_header, std::move(input_chunk));

        Block output_header = {ColumnWithTypeAndName{int32_type->createColumn(), int32_type, "value"}};
        ColumnsDescription output_columns{ColumnDescription{"value", int32_type}};

        const String python_source = R"PY(
import builtins

def py_transform(i):
    def gen():
        yield [(x,) for x in i]
        builtins._tp_python_table_transform_blocked = True
        while True:
            pass
    return gen()
)PY";

        auto transform = std::make_shared<PythonTableTransform>(
            source->getPort().getHeader(),
            output_header,
            python_source,
            "py_transform",
            Names{"i"},
            output_columns,
            Names{"value"},
            PythonTableMode::Streaming);

        auto sink = std::make_shared<CollectingBlocksSink>(transform->getOutputPort().getHeader());

        connect(source->getPort(), transform->getInputPort());
        connect(transform->getOutputPort(), sink->getPort());

        auto processors = std::make_shared<Processors>();
        processors->emplace_back(source);
        processors->emplace_back(transform);
        processors->emplace_back(sink);

        QueryStatusPtr element;
        PipelineExecutor executor(processors, element);

        std::mutex finished_mutex;
        std::condition_variable finished_cv;
        bool finished = false;
        std::exception_ptr execution_exception;

        {
            cpython::GILGuard release_gil;
        }

        std::thread executor_thread([&] {
            try
            {
                executor.execute(1);
            }
            catch (...)
            {
                execution_exception = std::current_exception();
            }

            {
                std::lock_guard lock(finished_mutex);
                finished = true;
            }
            finished_cv.notify_one();
        });

        SCOPE_EXIT({
            executor.cancel();
            if (executor_thread.joinable())
                executor_thread.join();
        });

        ASSERT_TRUE(waitForPythonFlag(blocked_flag, std::chrono::seconds(5)));

        executor.cancel();

        {
            std::unique_lock lock(finished_mutex);
            ASSERT_TRUE(finished_cv.wait_for(lock, std::chrono::seconds(2), [&] { return finished; }));
        }

        if (execution_exception)
        {
            try
            {
                std::rethrow_exception(execution_exception);
            }
            catch (const Exception & e)
            {
                EXPECT_EQ(e.code(), ErrorCodes::QUERY_WAS_CANCELLED) << e.displayText();
            }
            catch (...)
            {
                FAIL() << "Unexpected exception type";
            }
        }

        EXPECT_TRUE(sink->getBlocks().empty());
    });
}

#endif
