#include "config.h"

#if USE_PYTHON_UDF

#include <CPython/GILGuard.h>
#include <CPython/PyObjectPtr.h>
#include <CPython/Utils.h>

#include <CPython/tests/CPythonTest.h>

using namespace DB::cpython;

namespace
{
PyObjectPtr callNoArgs(const PyObjectPtr & callable)
{
    auto args = PyObjectPtr{PyTuple_New(0)};
    return executeObject(callable, args);
}

void closeCoroutineNoThrow(const PyObjectPtr & coro)
{
    if (!coro)
        return;

    PyObjectPtr close_result{PyObject_CallMethod(coro.get(), "close", nullptr)};
    if (!close_result && PyErr_Occurred())
        PyErr_Clear();
}
}

TEST_F(CPythonTest, isGeneratorRejectsCoroutine)
{
    {
        GILGuard gil_guard(true);
        ASSERT_EQ(
            PyRun_SimpleString(R"(
async def _proton_test_async_coro():
    return 1
)"),
            0);
    }

    assertNoLeak([]() {
        GILGuard gil_guard(true);

        auto py_func = getFunction("_proton_test_async_coro", "__main__");
        auto coro = callNoArgs(py_func);

        EXPECT_TRUE(isAsyncGeneratorOrCoroutine(coro));
        EXPECT_FALSE(isGenerator(coro));

        closeCoroutineNoThrow(coro);
    });
}

TEST_F(CPythonTest, isGeneratorRejectsAsyncGenerator)
{
    {
        GILGuard gil_guard(true);
        ASSERT_EQ(
            PyRun_SimpleString(R"(
async def _proton_test_async_gen():
    yield 1
)"),
            0);
    }

    assertNoLeak([]() {
        GILGuard gil_guard(true);

        auto py_func = getFunction("_proton_test_async_gen", "__main__");
        auto agen = callNoArgs(py_func);

        EXPECT_TRUE(isAsyncGeneratorOrCoroutine(agen));
        EXPECT_FALSE(isGenerator(agen));

        /// Avoid warnings about un-awaited aclose coroutine during test teardown.
        PyObjectPtr aclose_coro{PyObject_CallMethod(agen.get(), "aclose", nullptr)};
        if (aclose_coro)
            closeCoroutineNoThrow(aclose_coro);
        else if (PyErr_Occurred())
            PyErr_Clear();
    });
}

TEST_F(CPythonTest, isGeneratorAcceptsSyncGenerator)
{
    {
        GILGuard gil_guard(true);
        ASSERT_EQ(
            PyRun_SimpleString(R"(
def _proton_test_sync_gen():
    yield 1
)"),
            0);
    }

    assertNoLeak([]() {
        GILGuard gil_guard(true);

        auto py_func = getFunction("_proton_test_sync_gen", "__main__");
        auto gen = callNoArgs(py_func);

        EXPECT_FALSE(isAsyncGeneratorOrCoroutine(gen));
        EXPECT_TRUE(isGenerator(gen));
    });
}

#endif
