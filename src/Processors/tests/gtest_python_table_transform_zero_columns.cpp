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
extern const int UDF_RUNNING_ERROR;
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

long getPythonLongFlag(const char * attr_name, long default_value = 0)
{
    cpython::GILGuard gil_guard(/*use_need_cleanup=*/true);
    cpython::PyObjectPtr builtins{PyImport_ImportModule("builtins")};
    if (!builtins)
    {
        PyErr_Clear();
        return default_value;
    }

    cpython::PyObjectPtr value{PyObject_GetAttrString(builtins.get(), attr_name)};
    if (!value)
    {
        PyErr_Clear();
        return default_value;
    }

    long result = PyLong_AsLong(value.get());
    if (result == -1 && PyErr_Occurred())
    {
        PyErr_Clear();
        return default_value;
    }

    return result;
}

String getPythonStringFlag(const char * attr_name, const String & default_value = {})
{
    cpython::GILGuard gil_guard(/*use_need_cleanup=*/true);
    cpython::PyObjectPtr builtins{PyImport_ImportModule("builtins")};
    if (!builtins)
    {
        PyErr_Clear();
        return default_value;
    }

    cpython::PyObjectPtr value{PyObject_GetAttrString(builtins.get(), attr_name)};
    if (!value)
    {
        PyErr_Clear();
        return default_value;
    }

    Py_ssize_t size = 0;
    const char * data = PyUnicode_AsUTF8AndSize(value.get(), &size);
    if (!data)
    {
        PyErr_Clear();
        return default_value;
    }

    return String(data, static_cast<size_t>(size));
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

        auto session = cpython::PythonModuleSession::create(
            "PythonTableTransform",
            cpython::PythonFunction{
                .init_function_name = {},
                .init_parameters = {},
                .deinit_function_name = {},
                .flush_function_name = {},
                .entry_function_name = "py_transform",
                .source_code = python_source});
        auto transform = std::make_shared<PythonTableTransform>(
            source->getPort().getHeader(), output_header, Names{"i"}, output_columns, Names{}, PythonTableMode::Batch, session);

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

        auto session = cpython::PythonModuleSession::create(
            "PythonTableTransform",
            cpython::PythonFunction{
                .init_function_name = {},
                .init_parameters = {},
                .deinit_function_name = {},
                .flush_function_name = {},
                .entry_function_name = "py_transform",
                .source_code = python_source});
        auto transform = std::make_shared<PythonTableTransform>(
            source->getPort().getHeader(), output_header, Names{"i"}, output_columns, Names{"value"}, PythonTableMode::Streaming, session);

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

TEST_F(CPythonTest, PythonTableTransformRunsDeinitOnceOnNormalFinish)
{
    assertNoLeak([&]() {
        constexpr auto deinit_count_flag = "_tp_python_table_transform_deinit_count";

        clearPythonFlag(deinit_count_flag);
        SCOPE_EXIT({ clearPythonFlag(deinit_count_flag); });

        auto int32_type = std::make_shared<DataTypeInt32>();

        Block input_header = {ColumnWithTypeAndName{int32_type->createColumn(), int32_type, "i"}};

        auto col = ColumnInt32::create();
        col->insertValue(1);
        col->insertValue(2);

        Columns columns;
        columns.emplace_back(std::move(col));
        Chunk input_chunk(std::move(columns), 2);

        auto source = std::make_shared<SourceFromSingleChunk>(input_header, std::move(input_chunk));

        Block output_header = {ColumnWithTypeAndName{int32_type->createColumn(), int32_type, "value"}};
        ColumnsDescription output_columns{ColumnDescription{"value", int32_type}};

        const String python_source = R"PY(
import builtins

def deinit():
    builtins._tp_python_table_transform_deinit_count = getattr(builtins, "_tp_python_table_transform_deinit_count", 0) + 1

def py_transform(i):
    return [(x,) for x in i]
)PY";

        auto transform = std::make_shared<PythonTableTransform>(
            source->getPort().getHeader(),
            output_header,
            Names{"i"},
            output_columns,
            Names{"value"},
            PythonTableMode::Batch,
            cpython::PythonModuleSession::create(
                "PythonTableTransform",
                cpython::PythonFunction{
                    .init_function_name = {},
                    .init_parameters = {},
                    .deinit_function_name = "deinit",
                    .flush_function_name = {},
                    .entry_function_name = "py_transform",
                    .source_code = python_source}));

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

        EXPECT_EQ(sink->getRowCount(), 2U);
        EXPECT_EQ(getPythonLongFlag(deinit_count_flag, -1), 1);
    });
}

TEST_F(CPythonTest, PythonTableTransformPropagatesDeinitExceptionOnNormalFinish)
{
    assertNoLeak([&]() {
        constexpr auto deinit_count_flag = "_tp_python_table_transform_deinit_error_count";

        clearPythonFlag(deinit_count_flag);
        SCOPE_EXIT({ clearPythonFlag(deinit_count_flag); });

        auto int32_type = std::make_shared<DataTypeInt32>();

        Block input_header = {ColumnWithTypeAndName{int32_type->createColumn(), int32_type, "i"}};

        auto col = ColumnInt32::create();
        col->insertValue(7);

        Columns columns;
        columns.emplace_back(std::move(col));
        Chunk input_chunk(std::move(columns), 1);

        auto source = std::make_shared<SourceFromSingleChunk>(input_header, std::move(input_chunk));

        Block output_header = {ColumnWithTypeAndName{int32_type->createColumn(), int32_type, "value"}};
        ColumnsDescription output_columns{ColumnDescription{"value", int32_type}};

        const String python_source = R"PY(
import builtins

def deinit():
    builtins._tp_python_table_transform_deinit_error_count = getattr(builtins, "_tp_python_table_transform_deinit_error_count", 0) + 1
    raise RuntimeError("deinit boom")

def py_transform(i):
    return [(x,) for x in i]
)PY";

        auto transform = std::make_shared<PythonTableTransform>(
            source->getPort().getHeader(),
            output_header,
            Names{"i"},
            output_columns,
            Names{"value"},
            PythonTableMode::Batch,
            cpython::PythonModuleSession::create(
                "PythonTableTransform",
                cpython::PythonFunction{
                    .init_function_name = {},
                    .init_parameters = {},
                    .deinit_function_name = "deinit",
                    .flush_function_name = {},
                    .entry_function_name = "py_transform",
                    .source_code = python_source}));

        auto sink = std::make_shared<ValidatingRowCountSink>(transform->getOutputPort().getHeader());

        connect(source->getPort(), transform->getInputPort());
        connect(transform->getOutputPort(), sink->getPort());

        auto processors = std::make_shared<Processors>();
        processors->emplace_back(source);
        processors->emplace_back(transform);
        processors->emplace_back(sink);

        QueryStatusPtr element;
        PipelineExecutor executor(processors, element);

        try
        {
            executor.execute(1);
            FAIL() << "Expected DB::Exception";
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::UDF_RUNNING_ERROR);
            EXPECT_NE(e.displayText().find("deinit boom"), String::npos) << e.displayText();
        }

        EXPECT_EQ(getPythonLongFlag(deinit_count_flag, -1), 1);
    });
}

/// Verify that init_parameters is passed as a raw string — not parsed as JSON.
TEST_F(CPythonTest, PythonTableTransformInitParametersPassedAsRawString)
{
    assertNoLeak([&]() {
        constexpr auto received_flag = "_tp_python_table_transform_raw_init_param";

        clearPythonFlag(received_flag);
        SCOPE_EXIT({ clearPythonFlag(received_flag); });

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

def my_init(config):
    builtins._tp_python_table_transform_raw_init_param = config

def py_transform(i):
    return [(x,) for x in i]
)PY";

        auto session = cpython::PythonModuleSession::create(
            "PythonTableTransform",
            cpython::PythonFunction{
                .init_function_name = "my_init",
                .init_parameters = "key1=val1,key2=val2",
                .deinit_function_name = {},
                .flush_function_name = {},
                .entry_function_name = "py_transform",
                .source_code = python_source});
        auto transform = std::make_shared<PythonTableTransform>(
            source->getPort().getHeader(), output_header, Names{"i"}, output_columns, Names{"value"}, PythonTableMode::Batch, session);

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

        EXPECT_EQ(sink->getRowCount(), 1U);
        EXPECT_EQ(getPythonStringFlag(received_flag), "key1=val1,key2=val2");
    });
}

/// Verify that a JSON string is passed through verbatim — the C++ layer never parses it.
TEST_F(CPythonTest, PythonTableTransformInitParametersPassedAsJSON)
{
    assertNoLeak([&]() {
        constexpr auto received_flag = "_tp_python_table_transform_json_init_param";

        clearPythonFlag(received_flag);
        SCOPE_EXIT({ clearPythonFlag(received_flag); });

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

        const String json_params = R"({"host":"localhost","port":9092,"nested":{"flag":true}})";

        const String python_source = R"PY(
import builtins

def my_init(config):
    builtins._tp_python_table_transform_json_init_param = config

def py_transform(i):
    return [(x,) for x in i]
)PY";

        auto session = cpython::PythonModuleSession::create(
            "PythonTableTransform",
            cpython::PythonFunction{
                .init_function_name = "my_init",
                .init_parameters = json_params,
                .deinit_function_name = {},
                .flush_function_name = {},
                .entry_function_name = "py_transform",
                .source_code = python_source});
        auto transform = std::make_shared<PythonTableTransform>(
            source->getPort().getHeader(), output_header, Names{"i"}, output_columns, Names{"value"}, PythonTableMode::Batch, session);

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

        EXPECT_EQ(sink->getRowCount(), 1U);
        EXPECT_EQ(getPythonStringFlag(received_flag), json_params);
    });
}

/// Verify that init_parameters with special characters (quotes, unicode) are preserved.
TEST_F(CPythonTest, PythonTableTransformInitParametersSpecialCharacters)
{
    assertNoLeak([&]() {
        constexpr auto received_flag = "_tp_python_table_transform_special_init_param";

        clearPythonFlag(received_flag);
        SCOPE_EXIT({ clearPythonFlag(received_flag); });

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

        /// String with embedded double-quotes, newline, tab, and unicode.
        const String tricky_params = "name=\"caf\xC3\xA9\"\tcount=3\nmode=turbo";

        const String python_source = R"PY(
import builtins

def my_init(config):
    builtins._tp_python_table_transform_special_init_param = config

def py_transform(i):
    return [(x,) for x in i]
)PY";

        auto session = cpython::PythonModuleSession::create(
            "PythonTableTransform",
            cpython::PythonFunction{
                .init_function_name = "my_init",
                .init_parameters = tricky_params,
                .deinit_function_name = {},
                .flush_function_name = {},
                .entry_function_name = "py_transform",
                .source_code = python_source});
        auto transform = std::make_shared<PythonTableTransform>(
            source->getPort().getHeader(), output_header, Names{"i"}, output_columns, Names{"value"}, PythonTableMode::Batch, session);

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

        EXPECT_EQ(sink->getRowCount(), 1U);
        EXPECT_EQ(getPythonStringFlag(received_flag), tricky_params);
    });
}

/// Verify that init is called with zero args when init_function_name is set but init_parameters is empty.
TEST_F(CPythonTest, PythonTableTransformInitWithNoParameters)
{
    assertNoLeak([&]() {
        constexpr auto received_flag = "_tp_python_table_transform_init_no_params";

        clearPythonFlag(received_flag);
        SCOPE_EXIT({ clearPythonFlag(received_flag); });

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

def my_init():
    builtins._tp_python_table_transform_init_no_params = "called"

def py_transform(i):
    return [(x,) for x in i]
)PY";

        auto session = cpython::PythonModuleSession::create(
            "PythonTableTransform",
            cpython::PythonFunction{
                .init_function_name = "my_init",
                .init_parameters = {},
                .deinit_function_name = {},
                .flush_function_name = {},
                .entry_function_name = "py_transform",
                .source_code = python_source});
        auto transform = std::make_shared<PythonTableTransform>(
            source->getPort().getHeader(), output_header, Names{"i"}, output_columns, Names{"value"}, PythonTableMode::Batch, session);

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

        EXPECT_EQ(sink->getRowCount(), 1U);
        EXPECT_EQ(getPythonStringFlag(received_flag), "called");
    });
}

/// Verify that init + deinit work together with init_parameters.
TEST_F(CPythonTest, PythonTableTransformInitWithParametersAndDeinit)
{
    assertNoLeak([&]() {
        constexpr auto init_flag = "_tp_python_table_transform_init_deinit_param";
        constexpr auto deinit_flag = "_tp_python_table_transform_init_deinit_cleanup";

        clearPythonFlag(init_flag);
        clearPythonFlag(deinit_flag);
        SCOPE_EXIT({
            clearPythonFlag(init_flag);
            clearPythonFlag(deinit_flag);
        });

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

def my_init(config):
    builtins._tp_python_table_transform_init_deinit_param = config

def my_deinit():
    builtins._tp_python_table_transform_init_deinit_cleanup = "cleaned"

def py_transform(i):
    return [(x,) for x in i]
)PY";

        auto transform = std::make_shared<PythonTableTransform>(
            source->getPort().getHeader(),
            output_header,
            Names{"i"},
            output_columns,
            Names{"value"},
            PythonTableMode::Batch,
            cpython::PythonModuleSession::create(
                "PythonTableTransform",
                cpython::PythonFunction{
                    .init_function_name = "my_init",
                    .init_parameters = "conn=db://host:5432/mydb",
                    .deinit_function_name = "my_deinit",
                    .flush_function_name = {},
                    .entry_function_name = "py_transform",
                    .source_code = python_source}));

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

        EXPECT_EQ(sink->getRowCount(), 1U);
        EXPECT_EQ(getPythonStringFlag(init_flag), "conn=db://host:5432/mydb");
        EXPECT_EQ(getPythonStringFlag(deinit_flag), "cleaned");
    });
}

#endif
