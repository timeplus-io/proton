#include "config.h"

#if USE_PYTHON_UDF

#include <gtest/gtest.h>

#include <CPython/GILGuard.h>
#include <CPython/PyObjectPtr.h>
#include <CPython/Utils.h>
#include <CPython/tests/CPythonTest.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/ISink.h>
#include <Processors/Sources/PythonStreamingSource.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>

#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>

#include <datetime.h>

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

class NotifyingCollectBlocksSink final : public ISink
{
public:
    explicit NotifyingCollectBlocksSink(Block header) : ISink(std::move(header), ProcessorID::EmptySinkID) { }

    String getName() const override { return "NotifyingCollectBlocksSink"; }

    bool waitForBlocks(size_t count, std::chrono::milliseconds timeout)
    {
        std::unique_lock lock(mutex);
        return cv.wait_for(lock, timeout, [&] { return blocks.size() >= count; });
    }

    const std::vector<Block> & getBlocks() const { return blocks; }

protected:
    void consume(Chunk chunk) override
    {
        std::lock_guard lock(mutex);
        blocks.emplace_back(getPort().getHeader().cloneWithColumns(chunk.detachColumns()));
        cv.notify_all();
    }

private:
    std::mutex mutex;
    std::condition_variable cv;
    std::vector<Block> blocks;
};

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

        auto source = std::make_shared<PythonStreamingSource>(header, std::move(iterator), tuple_type, cpython::PythonModuleSessionPtr{});
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

        auto source = std::make_shared<PythonStreamingSource>(header, std::move(iterator), tuple_type, cpython::PythonModuleSessionPtr{});
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

TEST_F(CPythonTest, PythonStreamingSourceProjectionRejectsWrongArity)
{
    assertNoLeak([&]() {
        auto string_type = std::make_shared<DataTypeString>();
        auto int32_type = std::make_shared<DataTypeInt32>();

        DataTypes element_types = {string_type, int32_type};
        Strings element_names = {"type", "value"};
        auto tuple_type = std::make_shared<DataTypeTuple>(element_types, element_names);

        /// Project only the first column, but the generator returns rows of the wrong arity (1 instead of 2).
        Block header = {ColumnWithTypeAndName{string_type->createColumn(), string_type, "type"}};

        cpython::PyObjectPtr iterator;
        {
            cpython::GILGuard gil_guard(/*use_need_cleanup=*/true);

            cpython::PyObjectPtr rows{PyList_New(1)};
            ASSERT_TRUE(rows);

            PyObject * row = PyList_New(1);
            ASSERT_TRUE(row);
            PyList_SET_ITEM(row, 0, PyUnicode_FromString("ticker"));

            PyList_SET_ITEM(rows.get(), 0, row);

            iterator = cpython::PyObjectPtr{PyObject_GetIter(rows.get())};
            ASSERT_TRUE(iterator);
        }

        auto source = std::make_shared<PythonStreamingSource>(header, std::move(iterator), tuple_type, cpython::PythonModuleSessionPtr{});
        auto sink = std::make_shared<CollectBlocksSink>(source->getPort().getHeader());

        connect(source->getPort(), sink->getPort());

        auto processors = std::make_shared<Processors>();
        processors->emplace_back(source);
        processors->emplace_back(sink);

        QueryStatusPtr element;
        PipelineExecutor executor(processors, element);

        try
        {
            executor.execute(1);
            FAIL() << "Expected DB::Exception due to row arity mismatch";
        }
        catch (const DB::Exception & e)
        {
            EXPECT_EQ(e.code(), DB::ErrorCodes::UDF_RUNNING_ERROR);
        }
    });
}

TEST_F(CPythonTest, PythonStreamingSourcePreservesRowCountWhenNoColumnsProjected)
{
    assertNoLeak([&]() {
        auto string_type = std::make_shared<DataTypeString>();
        auto int32_type = std::make_shared<DataTypeInt32>();

        DataTypes element_types = {string_type, int32_type};
        Strings element_names = {"type", "value"};
        auto tuple_type = std::make_shared<DataTypeTuple>(element_types, element_names);

        Block header;

        cpython::PyObjectPtr iterator;
        {
            cpython::GILGuard gil_guard(/*use_need_cleanup=*/true);

            cpython::PyObjectPtr rows{PyList_New(3)};
            ASSERT_TRUE(rows);

            auto make_row = [](const char * type_value, int int_value) {
                PyObject * row = PyTuple_New(2);
                if (!row)
                    return row;
                PyTuple_SET_ITEM(row, 0, PyUnicode_FromString(type_value));
                PyTuple_SET_ITEM(row, 1, PyLong_FromLong(int_value));
                return row;
            };

            PyList_SET_ITEM(rows.get(), 0, make_row("a", 1));
            PyList_SET_ITEM(rows.get(), 1, make_row("b", 2));
            PyList_SET_ITEM(rows.get(), 2, make_row("c", 3));

            iterator = cpython::PyObjectPtr{PyObject_GetIter(rows.get())};
            ASSERT_TRUE(iterator);
        }

        auto source = std::make_shared<PythonStreamingSource>(header, std::move(iterator), tuple_type, cpython::PythonModuleSessionPtr{});
        auto sink = std::make_shared<RowCountSink>(source->getPort().getHeader());

        connect(source->getPort(), sink->getPort());

        auto processors = std::make_shared<Processors>();
        processors->emplace_back(source);
        processors->emplace_back(sink);

        QueryStatusPtr element;
        PipelineExecutor executor(processors, element);
        executor.execute(1);

        EXPECT_EQ(sink->getColumnCount(), 0U);
        EXPECT_EQ(sink->getRowCount(), 3U);
    });
}

TEST_F(CPythonTest, PythonStreamingSourceNoColumnsRejectsWrongArity)
{
    assertNoLeak([&]() {
        auto string_type = std::make_shared<DataTypeString>();
        auto int32_type = std::make_shared<DataTypeInt32>();

        DataTypes element_types = {string_type, int32_type};
        Strings element_names = {"type", "value"};
        auto tuple_type = std::make_shared<DataTypeTuple>(element_types, element_names);

        Block header; /// no columns projected

        cpython::PyObjectPtr iterator;
        {
            cpython::GILGuard gil_guard(/*use_need_cleanup=*/true);

            cpython::PyObjectPtr rows{PyList_New(1)};
            ASSERT_TRUE(rows);

            /// Wrong arity row (1 instead of 2).
            PyObject * row = PyTuple_New(1);
            ASSERT_TRUE(row);
            PyTuple_SET_ITEM(row, 0, PyUnicode_FromString("ticker"));
            PyList_SET_ITEM(rows.get(), 0, row);

            iterator = cpython::PyObjectPtr{PyObject_GetIter(rows.get())};
            ASSERT_TRUE(iterator);
        }

        auto source = std::make_shared<PythonStreamingSource>(header, std::move(iterator), tuple_type, cpython::PythonModuleSessionPtr{});
        auto sink = std::make_shared<RowCountSink>(source->getPort().getHeader());

        connect(source->getPort(), sink->getPort());

        auto processors = std::make_shared<Processors>();
        processors->emplace_back(source);
        processors->emplace_back(sink);

        QueryStatusPtr element;
        PipelineExecutor executor(processors, element);

        try
        {
            executor.execute(1);
            FAIL() << "Expected DB::Exception due to row arity mismatch";
        }
        catch (const DB::Exception & e)
        {
            EXPECT_EQ(e.code(), DB::ErrorCodes::UDF_RUNNING_ERROR);
        }
    });
}

TEST_F(CPythonTest, PythonStreamingSourceSkipsEmptyBatches)
{
    assertNoLeak([&]() {
        auto string_type = std::make_shared<DataTypeString>();
        auto int32_type = std::make_shared<DataTypeInt32>();

        DataTypes element_types = {string_type, int32_type};
        Strings element_names = {"type", "value"};
        auto tuple_type = std::make_shared<DataTypeTuple>(element_types, element_names);

        Block header = {ColumnWithTypeAndName{string_type->createColumn(), string_type, "type"}};

        cpython::PyObjectPtr iterator;
        {
            cpython::GILGuard gil_guard(/*use_need_cleanup=*/true);

            cpython::PyObjectPtr batches{PyList_New(2)};
            ASSERT_TRUE(batches);

            /// First batch is empty ([]), second batch has one row.
            PyObject * empty_batch = PyList_New(0);
            ASSERT_TRUE(empty_batch);
            PyList_SET_ITEM(batches.get(), 0, empty_batch);

            PyObject * non_empty_batch = PyList_New(1);
            ASSERT_TRUE(non_empty_batch);

            PyObject * row = PyTuple_New(2);
            ASSERT_TRUE(row);
            PyTuple_SET_ITEM(row, 0, PyUnicode_FromString("ticker"));
            PyTuple_SET_ITEM(row, 1, PyLong_FromLong(42));
            PyList_SET_ITEM(non_empty_batch, 0, row);
            PyList_SET_ITEM(batches.get(), 1, non_empty_batch);

            iterator = cpython::PyObjectPtr{PyObject_GetIter(batches.get())};
            ASSERT_TRUE(iterator);
        }

        auto source = std::make_shared<PythonStreamingSource>(header, std::move(iterator), tuple_type, cpython::PythonModuleSessionPtr{});
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

TEST_F(CPythonTest, PythonStreamingSourceCancelUnblocksIterator)
{
    assertNoLeak([&]() {
        auto string_type = std::make_shared<DataTypeString>();

        DataTypes element_types = {string_type};
        Strings element_names = {"type"};
        auto tuple_type = std::make_shared<DataTypeTuple>(element_types, element_names);

        Block header = {ColumnWithTypeAndName{string_type->createColumn(), string_type, "type"}};

        cpython::PyObjectPtr iterator;
        cpython::PyObjectPtr iterator_owner;
        String module_name;
        {
            cpython::GILGuard gil_guard(/*use_need_cleanup=*/true);

            module_name = "test_cancel_" + cpython::randomModuleName();
            const std::string python_source = R"PY(
class BlockingIterator:
    def __init__(self):
        self._cancelled = False
        self._yielded = False

    def __iter__(self):
        return self

    def cancel(self):
        self._cancelled = True

    def __next__(self):
        if self._cancelled:
            raise StopIteration
        if not self._yielded:
            self._yielded = True
            return [("ticker",)]
        while not self._cancelled:
            pass
        raise StopIteration
)PY";

            auto byte_code = cpython::compile(python_source);
            cpython::executeByteCode(byte_code, module_name);

            auto blocking_iter_class = cpython::getClass("BlockingIterator", module_name);
            iterator_owner = cpython::newInstance(blocking_iter_class);
            ASSERT_TRUE(iterator_owner);

            iterator = cpython::getIterator(iterator_owner);
            ASSERT_TRUE(iterator);
        }

        String first_type_value;
        bool got_first_block = false;
        {
            auto source
                = std::make_shared<PythonStreamingSource>(header, std::move(iterator), tuple_type, cpython::PythonModuleSessionPtr{});
            auto sink = std::make_shared<NotifyingCollectBlocksSink>(source->getPort().getHeader());

            connect(source->getPort(), sink->getPort());

            auto processors = std::make_shared<Processors>();
            processors->emplace_back(source);
            processors->emplace_back(sink);

            QueryStatusPtr element;
            PipelineExecutor executor(processors, element);

            std::mutex finished_mutex;
            std::condition_variable finished_cv;
            bool finished = false;
            std::exception_ptr execution_exception;

            /// Release the GIL on the main test thread so the executor thread can run Python.
            /// Otherwise the executor thread may block forever on PyGILState_Ensure().
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

            got_first_block = sink->waitForBlocks(1, std::chrono::seconds(5));
            EXPECT_TRUE(got_first_block);

            executor.cancel();

            {
                std::unique_lock lock(finished_mutex);
                if (!finished_cv.wait_for(lock, std::chrono::seconds(1), [&] { return finished; }))
                {
                    /// Avoid hanging the test if cancellation breaks: unblock the iterator explicitly.
                    cpython::GILGuard gil_guard(/*use_need_cleanup=*/true);
                    if (iterator_owner && PyObject_HasAttrString(iterator_owner.get(), "cancel"))
                    {
                        cpython::PyObjectPtr cancel_method{PyObject_GetAttrString(iterator_owner.get(), "cancel")};
                        EXPECT_TRUE(cancel_method);
                        cpython::PyObjectPtr cancel_result{PyObject_CallObject(cancel_method.get(), nullptr)};
                        if (!cancel_result)
                            PyErr_Clear();
                    }

                    EXPECT_TRUE(finished_cv.wait_for(lock, std::chrono::seconds(2), [&] { return finished; }));
                }
            }

            if (execution_exception)
            {
                try
                {
                    std::rethrow_exception(execution_exception);
                }
                catch (const Exception & e)
                {
                    if (e.code() != ErrorCodes::QUERY_WAS_CANCELLED)
                        ADD_FAILURE() << "Unexpected exception: " << e.displayText();
                }
                catch (...)
                {
                    ADD_FAILURE() << "Unexpected exception type";
                }
            }

            {
                cpython::GILGuard gil_guard(/*use_need_cleanup=*/true);
                iterator_owner.reset();
            }

            if (got_first_block)
            {
                EXPECT_EQ(sink->getBlocks().size(), 1U);
                if (!sink->getBlocks().empty())
                {
                    const auto & block = sink->getBlocks().front();
                    EXPECT_EQ(block.columns(), 1U);
                    EXPECT_TRUE(block.has("type"));
                    if (block.has("type"))
                    {
                        const auto & col = assert_cast<const ColumnString &>(*block.getByName("type").column);
                        if (col.size() > 0)
                            first_type_value = col.getDataAt(0).toString();
                    }
                }
            }
        }

        /// Unload the manually-created module and collect cycles so assertNoLeak doesn't
        /// report false-positives from class MRO tuples.
        {
            cpython::GILGuard gil_guard(/*use_need_cleanup=*/true);
            if (cpython::hasModule(module_name))
                cpython::unloadModule(module_name);
            cpython::PyObjectPtr gc_module{PyImport_ImportModule("gc")};
            ASSERT_TRUE(gc_module);
            cpython::PyObjectPtr collect_result{PyObject_CallMethod(gc_module.get(), "collect", nullptr)};
            if (!collect_result)
                PyErr_Clear();
        }

        if (got_first_block)
            EXPECT_EQ(first_type_value, "ticker");
    });
}

TEST_F(CPythonTest, PythonStreamingSourceCancelWithAsyncInterruptStopsCleanly)
{
    assertNoLeak([&]() {
        auto string_type = std::make_shared<DataTypeString>();

        DataTypes element_types = {string_type};
        Strings element_names = {"type"};
        auto tuple_type = std::make_shared<DataTypeTuple>(element_types, element_names);

        Block header = {ColumnWithTypeAndName{string_type->createColumn(), string_type, "type"}};

        cpython::PyObjectPtr iterator;
        cpython::PyObjectPtr iterator_owner;
        String module_name;
        {
            cpython::GILGuard gil_guard(/*use_need_cleanup=*/true);

            module_name = "test_async_cancel_" + cpython::randomModuleName();
            const std::string python_source = R"PY(
class BlockingIteratorWithoutCancel:
    def __init__(self):
        self._yielded = False
        self._stop_for_test = False

    def __iter__(self):
        return self

    def stop_for_test(self):
        self._stop_for_test = True

    def __next__(self):
        if not self._yielded:
            self._yielded = True
            return [("ticker",)]

        while not self._stop_for_test:
            pass
        raise StopIteration
)PY";

            auto byte_code = cpython::compile(python_source);
            cpython::executeByteCode(byte_code, module_name);

            auto blocking_iter_class = cpython::getClass("BlockingIteratorWithoutCancel", module_name);
            iterator_owner = cpython::newInstance(blocking_iter_class);
            ASSERT_TRUE(iterator_owner);

            iterator = cpython::getIterator(iterator_owner);
            ASSERT_TRUE(iterator);
        }

        {
            auto source
                = std::make_shared<PythonStreamingSource>(header, std::move(iterator), tuple_type, cpython::PythonModuleSessionPtr{});
            auto sink = std::make_shared<NotifyingCollectBlocksSink>(source->getPort().getHeader());

            connect(source->getPort(), sink->getPort());

            auto processors = std::make_shared<Processors>();
            processors->emplace_back(source);
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

            ASSERT_TRUE(sink->waitForBlocks(1, std::chrono::seconds(5)));

            executor.cancel();

            bool finished_after_cancel = false;
            {
                std::unique_lock lock(finished_mutex);
                finished_after_cancel = finished_cv.wait_for(lock, std::chrono::milliseconds(500), [&] { return finished; });
            }

            if (!finished_after_cancel)
            {
                cpython::GILGuard gil_guard(/*use_need_cleanup=*/true);
                cpython::PyObjectPtr stop_method{PyObject_GetAttrString(iterator_owner.get(), "stop_for_test")};
                ASSERT_TRUE(stop_method);
                cpython::PyObjectPtr stop_result{PyObject_CallObject(stop_method.get(), nullptr)};
                if (!stop_result)
                    PyErr_Clear();

                std::unique_lock lock(finished_mutex);
                EXPECT_TRUE(finished_cv.wait_for(lock, std::chrono::seconds(2), [&] { return finished; }));
            }

            if constexpr (!cpython::GILGuard::buildSupportsFreeThreading())
            {
                /// GIL build: onCancel injects KeyboardInterrupt, which interrupts even a
                /// non-cooperative loop, so executor.cancel() alone always completes
                /// cancellation before the 500ms wait above expires.
                EXPECT_TRUE(finished_after_cancel);
            }
            /// Free-threaded build: PyThreadState_SetAsyncExc is compiled out of onCancel
            /// (under FT it would race a thread-pool recycle and misroute the
            /// KeyboardInterrupt onto an unrelated query's recycled tstate). With it gone,
            /// executor.cancel() can only end the iterator when cancel_requested is observed
            /// at a generate() loop boundary — a timing race against the iterator re-entering
            /// its blocking `while: pass`. finished_after_cancel is therefore non-deterministic
            /// under FT (true when cancel wins the race, false otherwise), so it is not
            /// asserted here. When cancel loses the race, the stop_for_test() cooperative path
            /// above unblocks the iterator; the clean-shutdown join and the single-block check
            /// below are the FT invariants.

            if (execution_exception)
            {
                try
                {
                    std::rethrow_exception(execution_exception);
                }
                catch (const Exception & e)
                {
                    ADD_FAILURE() << "Unexpected exception: " << e.displayText();
                }
                catch (...)
                {
                    ADD_FAILURE() << "Unexpected exception type";
                }
            }

            EXPECT_EQ(sink->getBlocks().size(), 1U);
        }

        {
            cpython::GILGuard gil_guard(/*use_need_cleanup=*/true);
            iterator_owner.reset();
            if (cpython::hasModule(module_name))
                cpython::unloadModule(module_name);
            cpython::PyObjectPtr gc_module{PyImport_ImportModule("gc")};
            ASSERT_TRUE(gc_module);
            cpython::PyObjectPtr collect_result{PyObject_CallMethod(gc_module.get(), "collect", nullptr)};
            if (!collect_result)
                PyErr_Clear();
        }
    });
}

#endif
