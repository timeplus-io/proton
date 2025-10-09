#pragma once

#include <Python.h>

#include <functional>
#include <memory>

namespace DB::cpython
{
class PyObjectPtr : public std::unique_ptr<PyObject, std::function<void(PyObject *)>>
{
public:
    using Base = std::unique_ptr<PyObject, std::function<void(PyObject *)>>;
    explicit PyObjectPtr(PyObject * obj = nullptr) : Base(obj, [](PyObject * obj_) { Py_XDECREF(obj_); }) { }

    static PyObjectPtr borrow(PyObject * obj)
    {
        Py_XINCREF(obj); /// PyObjectPtr doesn't own the python obj
        return PyObjectPtr(obj);
    }
};
}
