#pragma once

#include <CPython/PyObjectPtr.h>
#include <Columns/ColumnVector.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>

#include <Python.h>

namespace DB
{

namespace cpython
{

/// column -> list
PyObjectPtr convertColumnToPythonList(const IColumn & column, const DataTypePtr & type);
PyObjectPtr convertColumnToPythonList(const IColumn & column, const DataTypePtr & type, UInt64 offset, UInt64 size);

/// list -> column, reponsible for releasing the memory of the list, hand over the ownership of the list to the function
ColumnPtr convertPythonListToColumn(const PyObjectPtr & py_list, const DataTypePtr & type);

void convertPythonListToColumn(IColumn & column, const PyObjectPtr & py_list, const DataTypePtr & type);

void insertPythonObjectToColumn(IColumn & column, const PyObjectPtr & py_object, const DataTypePtr & type);

#if USE_NUMPY
/// column -> numpy array
PyObject * convertColumnToNumpyArray(const IColumn & column);
PyObject * convertColumnToNumpyArray(const ColumnWithTypeAndName & column_with_type);

/// numpy array -> column
void covertNumpyArrayToColumn(PyObject * py_array, IColumn & column);
ColumnPtr covertNumpyArrayToColumn(PyObject * py_array, const DataTypePtr & type);
#endif
}
}
