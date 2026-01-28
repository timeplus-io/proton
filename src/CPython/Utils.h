#pragma once

#include <CPython/PyObjectPtr.h>

#include <Python.h>

#include <string>

namespace DB::cpython
{
std::string randomModuleName();

std::string convertPyObjectToString(const PyObjectPtr & obj);

bool hasException();

std::string getExceptionMessage();

PyObjectPtr getAttr(const PyObjectPtr & obj, const std::string & attr);

PyObjectPtr tryGetAttr(const PyObjectPtr & obj, const std::string & attr);

PyObjectPtr importModule(const std::string & module_name);

PyObjectPtr getOrAddModule(const std::string & module_name);

void unloadModule(const std::string & module_name);

PyObjectPtr getModule(const std::string & module_name);

bool hasModule(const std::string & module_name);

std::string getObjectType(const PyObjectPtr & obj);

PyObjectPtr getFunction(const std::string & func_name, const std::string & module_name);

PyObjectPtr getClass(const std::string & class_name, const std::string & module_name);

void unloadClass(const PyObjectPtr & py_class);

PyObjectPtr newInstance(const PyObjectPtr & py_class, const PyObjectPtr & args = PyObjectPtr{});

PyObjectPtr getInstanceMethod(const PyObjectPtr & py_instance, const std::string & method_name);

PyObjectPtr compile(const std::string & source);

PyObjectPtr executeByteCode(const PyObjectPtr & byte_code, const std::string & module_name);

PyObjectPtr executeObject(const PyObjectPtr & obj, const PyObjectPtr & args = PyObjectPtr{});

/// Check if Python object is a generator or iterator
bool isGenerator(const PyObjectPtr & obj);

/// Check if Python object is an async coroutine or async generator.
/// These objects are not compatible with synchronous iteration via PyIter_Next.
bool isAsyncGeneratorOrCoroutine(const PyObjectPtr & obj);

/// Check if Python object is iterable (has __iter__ method)
bool isIterable(const PyObjectPtr & obj);

/// Get iterator from an iterable object
PyObjectPtr getIterator(const PyObjectPtr & obj);

/// Get next item from iterator (returns empty PyObjectPtr when exhausted)
PyObjectPtr iterNext(const PyObjectPtr & iterator);

/// Normalize a Python rows object into a list of tuples for tuple conversion.
/// If tuple_size == 1, allow list/tuple-of-scalars or scalar values by wrapping into 1-element tuples.
PyObjectPtr normalizePythonListForTuple(const PyObjectPtr & py_list, size_t tuple_size);
}
