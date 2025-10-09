#pragma once

#include <CPython/PyObjectPtr.h>

#include <Python.h>
#include <gtest/gtest.h>

#include <unordered_set>

class CPythonTest : public ::testing::Test
{
protected:
    std::unordered_set<PyObject *> before_objects;

    void SetUp() override { Py_Initialize(); }

    void collectObjects()
    {
        before_objects.clear();

        /// Get initial objects
        DB::cpython::PyObjectPtr gc_module{PyImport_ImportModule("gc")};
        if (gc_module)
        {
            DB::cpython::PyObjectPtr gc_get_objects{PyObject_GetAttrString(gc_module.get(), "get_objects")};
            if (gc_get_objects && PyCallable_Check(gc_get_objects.get()))
            {
                DB::cpython::PyObjectPtr objects{PyObject_CallObject(gc_get_objects.get(), nullptr)};
                if (objects)
                {
                    Py_ssize_t size = PyList_Size(objects.get());
                    for (Py_ssize_t i = 0; i < size; ++i)
                    {
                        before_objects.insert(PyList_GetItem(objects.get(), i));
                    }
                }
            }
        }
    }

    void assertObjectLeak()
    {
        /// Check for Python object leaks
        DB::cpython::PyObjectPtr gc_module{PyImport_ImportModule("gc")};
        if (gc_module)
        {
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
        collectObjects();
        func();
        assertObjectLeak();
    }

    void TearDown() override
    {
        /// Check for any python exceptions before finalizing
        if (PyErr_Occurred())
        {
            PyErr_Print();
            ADD_FAILURE() << "TearDown: Python error occurred during test.";
        }

        Py_Finalize();
    }
};
