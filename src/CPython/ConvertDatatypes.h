#include <Python.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <Columns/ColumnVector.h>
#include <Core/ColumnWithTypeAndName.h>
// #define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION
// #include <numpy/arrayobject.h>



namespace DB
{

namespace cpython
{

/// column -> list(temporary solution for testing, maybe converting to numpy array is better)
/// TODO: convert to numpy array
/// Final Purpose: 1. faster 2. no memory copy
PyObject * convertColumnToPythonList(const ColumnWithTypeAndName & column_with_type);

PyObject * convertColumnToPythonList(const ColumnPtr & column);


/// list -> column
ColumnPtr convertPythonListToColumn(PyObject * py_list, const DataTypePtr & arg_type);


// PyArrayObject * convertColumnToNumpyArray(const ColumnWithTypeAndName & column_with_type);



}
}

