#include <gtest/gtest.h>
#include <CPython/ConvertDatatypes.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>

#include <iostream>
using namespace DB;


TEST(PythonConvertDatatypes, to_python)
{
    setenv("PYTHONPATH", "/home/timeplus/lijianan/ssd/project/proton_sake/contrib/cpython-cmake/cpython/Lib", 1);

    Py_Initialize();
    auto column = ColumnUInt16::create();
    column->getData().push_back(1);
    column->getData().push_back(2);

    ColumnWithTypeAndName column_with_type(std::move(column), std::make_shared<DataTypeUInt16>(), "column");

    PyObject * py_list = cpython::convertColumnToPythonList(column_with_type);
    ASSERT_TRUE(py_list != nullptr);
    ASSERT_EQ(PyList_Size(py_list), 2);
    ASSERT_EQ(PyLong_AsLong(PyList_GetItem(py_list, 0)), 1);
    ASSERT_EQ(PyLong_AsLong(PyList_GetItem(py_list, 1)), 2);

    Py_DECREF(py_list);
    Py_Finalize();
}


TEST(PythonConvertDatatypes, from_python)
{
    setenv("PYTHONPATH", "/home/timeplus/lijianan/ssd/project/proton_sake/contrib/cpython-cmake/cpython/Lib", 1);

    Py_Initialize();
    PyObject * py_list = PyList_New(2);
    PyList_SetItem(py_list, 0, PyLong_FromLong(1));
    PyList_SetItem(py_list, 1, PyLong_FromLong(2));

    auto column = cpython::convertPythonListToColumn(py_list, std::make_shared<DataTypeUInt16>());
    ASSERT_TRUE(column != nullptr);
    ASSERT_EQ(column->size(), 2);
    ASSERT_EQ(assert_cast<const ColumnUInt16 &>(*column).getData()[0], 1);
    ASSERT_EQ(assert_cast<const ColumnUInt16 &>(*column).getData()[1], 2);
    Py_DECREF(py_list);
    Py_Finalize();
}


// TEST(PythonConvertDatatypes, datasize)
// {
//     setenv("PYTHONPATH", "/home/timeplus/lijianan/ssd/project/proton_sake/contrib/cpython-cmake/cpython/Lib", 1);

//     Py_Initialize();

//     std::cout << "1" << std::endl;
//     auto col_int32 = ColumnInt32::create();
//     col_int32->getData().push_back(1);
//     col_int32->getData().push_back(2);
//     col_int32->getData().push_back(3);

//     auto * address = col_int32->getData().data();
    
//     std::cout << "2" << std::endl;

//     PyArrayObject * numpy_array = cpython::convertColumnToNumpyArray(ColumnWithTypeAndName(std::move(col_int32), std::make_shared<DataTypeInt32>(), "column"));
//     ASSERT_TRUE(np_array != nullptr);
//     // Convert np_array to numpy array

//     // PyArrayObject* numpy_array = reinterpret_cast<PyArrayObject*>(np_array);

//     std::cout << "3" << std::endl;

//     // Access the numpy array
//     int* data = static_cast<int*>(PyArray_DATA(numpy_array));
//     int size = PyArray_SIZE(numpy_array);

//     ASSERT_EQ(size, 3);
//     ASSERT_EQ(data[0], 1);
//     ASSERT_EQ(data[1], 2);
//     ASSERT_EQ(data[2], 3);

//     ASSERT_EQ(data, address);
    
//     // clean up
//     Py_DECREF(np_array);
//     Py_Finalize();

// }
