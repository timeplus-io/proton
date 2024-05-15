#ifdef ENABLE_PYTHON_UDF
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

/// column -> list
PyObject * convertColumnToPythonList(const ColumnWithTypeAndName & column_with_type);
PyObject * convertColumnToPythonList(const IColumn & column, const DataTypePtr & type, UInt64 offset, UInt64 size);


/// list -> column, reponsible for releasing the memory of the list, hand over the ownership of the list to the function
void convertPythonListToColumn(PyObject * py_array, IColumn & column, const DataTypePtr & type, bool need_release = true);
ColumnPtr convertPythonListToColumn(PyObject * py_list, const DataTypePtr & type);


/// column -> numpy array
PyObject * convertColumnToNumpyArray(const IColumn & column);
PyObject * convertColumnToNumpyArray(const ColumnWithTypeAndName & column_with_type);

/// numpy array -> column
void covertNumpyArrayToColumn(PyObject * py_array, IColumn & column);
ColumnPtr covertNumpyArrayToColumn(PyObject * py_array, const DataTypePtr & type);

}
}
#endif

