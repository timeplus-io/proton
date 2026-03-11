#pragma once

#include <CPython/PyObjectPtr.h>
#include <CPython/PythonInterpreterInfo.h>

#include <Python.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <functional>
#include <string>
#include <unordered_set>

class CPythonTest : public ::testing::Test
{
protected:
    inline static wchar_t * program_name = nullptr;
    std::unordered_set<PyObject *> before_objects;

    static void SetUpTestSuite()
    {
        auto [interpreter_info, error_message] = DB::cpython::PythonInterpreterInfo::tryCollect("");
        if (!interpreter_info.has_value())
        {
            GTEST_SKIP() << "Python 3.10 interpreter not found, skipping CPython tests: " << error_message;
            return;
        }

        setenv("PYTHONHOME", interpreter_info->prefix.c_str(), /*overwrite=*/1);

        std::string python_path;
        for (const auto & sp : interpreter_info->site_packages)
        {
            if (!python_path.empty())
                python_path += ':';
            python_path += sp;
        }
        if (!python_path.empty())
            setenv("PYTHONPATH", python_path.c_str(), /*overwrite=*/1);

        if (program_name)
        {
            PyMem_RawFree(program_name);
            program_name = nullptr;
        }

        program_name = Py_DecodeLocale(interpreter_info->path.c_str(), nullptr);
        if (program_name)
            Py_SetProgramName(program_name);

        Py_Initialize();
    }

    static void TearDownTestSuite()
    {
        if (!Py_IsInitialized())
            return;

        /// Some tests (and production code paths) release the GIL with `PyEval_SaveThread()`,
        /// leaving the main thread without an active thread state. Always reacquire the GIL
        /// before touching the runtime and finalizing.
        PyGILState_STATE state = PyGILState_Ensure();

        if (PyErr_Occurred())
        {
            PyErr_Print();
            ADD_FAILURE() << "TearDownTestSuite: Python error occurred during tests.";
        }

        /// Do NOT call `PyGILState_Release` after `Py_Finalize()`.
        Py_Finalize();
        if (program_name)
        {
            PyMem_RawFree(program_name);
            program_name = nullptr;
        }
        (void)state;
    }

    static void warmUpReprThreadState()
    {
        /// `Py_ReprEnter` / `Py_ReprLeave` lazily allocate per-thread objects stored in the
        /// thread state's dict (key "Py_Repr"). Those objects persist for the lifetime of the
        /// thread state and would look like "leaks" if first created inside the code under test.
        ///
        /// Force this initialization before taking the baseline GC snapshot.
        (void)PyThreadState_GetDict();

        DB::cpython::PyObjectPtr list{PyList_New(1)};
        if (!list)
        {
            if (PyErr_Occurred())
                PyErr_Clear();
            return;
        }

        Py_INCREF(list.get());
        PyList_SET_ITEM(list.get(), 0, list.get()); /// steals the reference, creating a temporary self-cycle

        DB::cpython::PyObjectPtr repr{PyObject_Repr(list.get())};
        if (PyErr_Occurred())
            PyErr_Clear();

        Py_INCREF(Py_None);
        (void)PyList_SetItem(list.get(), 0, Py_None); /// steals; breaks the self-cycle by DECREF'ing the old item
        if (PyErr_Occurred())
            PyErr_Clear();
    }

    void collectObjectsAssumeGILHeld()
    {
        before_objects.clear();

        DB::cpython::PyObjectPtr gc_module{PyImport_ImportModule("gc")};
        if (!gc_module)
            return;

        /// Run a collection to reduce noise from cyclic garbage.
        DB::cpython::PyObjectPtr gc_collect{PyObject_GetAttrString(gc_module.get(), "collect")};
        if (gc_collect && PyCallable_Check(gc_collect.get()))
        {
            DB::cpython::PyObjectPtr unused{PyObject_CallObject(gc_collect.get(), nullptr)};
            if (PyErr_Occurred())
                PyErr_Clear();
        }
        else if (PyErr_Occurred())
        {
            PyErr_Clear();
        }

        DB::cpython::PyObjectPtr gc_get_objects{PyObject_GetAttrString(gc_module.get(), "get_objects")};
        if (!gc_get_objects || !PyCallable_Check(gc_get_objects.get()))
            return;

        DB::cpython::PyObjectPtr objects{PyObject_CallObject(gc_get_objects.get(), nullptr)};
        if (!objects)
            return;

        Py_ssize_t size = PyList_Size(objects.get());
        for (Py_ssize_t i = 0; i < size; ++i)
            before_objects.insert(PyList_GetItem(objects.get(), i));
    }

    void assertObjectLeakAssumeGILHeld()
    {
        DB::cpython::PyObjectPtr gc_module{PyImport_ImportModule("gc")};
        if (gc_module)
        {
            /// Run a collection to reduce noise from cyclic garbage.
            DB::cpython::PyObjectPtr gc_collect{PyObject_GetAttrString(gc_module.get(), "collect")};
            if (gc_collect && PyCallable_Check(gc_collect.get()))
            {
                DB::cpython::PyObjectPtr unused{PyObject_CallObject(gc_collect.get(), nullptr)};
                if (PyErr_Occurred())
                    PyErr_Clear();
            }
            else if (PyErr_Occurred())
            {
                PyErr_Clear();
            }

            DB::cpython::PyObjectPtr gc_get_objects{PyObject_GetAttrString(gc_module.get(), "get_objects")};
            if (gc_get_objects && PyCallable_Check(gc_get_objects.get()))
            {
                DB::cpython::PyObjectPtr after_gc_objects{PyObject_CallObject(gc_get_objects.get(), nullptr)};
                if (after_gc_objects)
                {
                    std::unordered_set<PyObject *> after_set;
                    Py_ssize_t after_size = PyList_Size(after_gc_objects.get());
                    for (Py_ssize_t i = 0; i < after_size; ++i)
                    {
                        after_set.insert(PyList_GetItem(after_gc_objects.get(), i));
                    }

                    for (auto obj : after_set)
                    {
                        if (before_objects.find(obj) == before_objects.end())
                        {
                            ADD_FAILURE() << "Leaked object detected: ";
                            PyObject_Print(obj, stderr, 0);
                            fprintf(stderr, "\n");
                        }
                    }
                }
            }
        }
    }

    void assertNoLeak(std::function<void()> func)
    {
        {
            PyGILState_STATE state = PyGILState_Ensure();
            warmUpReprThreadState();
            collectObjectsAssumeGILHeld();
            PyGILState_Release(state);
        }

        func();

        {
            PyGILState_STATE state = PyGILState_Ensure();
            assertObjectLeakAssumeGILHeld();
            PyGILState_Release(state);
        }
    }

    void SetUp() override
    {
        if (!Py_IsInitialized())
            GTEST_SKIP() << "Python 3.10 interpreter not found, skipping CPython tests";
    }

    void TearDown() override
    {
        if (!Py_IsInitialized())
            return;

        PyGILState_STATE state = PyGILState_Ensure();

        if (PyErr_Occurred())
        {
            PyErr_Print();
            ADD_FAILURE() << "TearDown: Python error occurred during test.";
        }

        PyGILState_Release(state);
    }
};
