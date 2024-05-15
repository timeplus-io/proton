#ifdef ENABLE_PYTHON_UDF
#ifdef HAS_RESERVED_IDENTIFIER
#pragma clang diagnostic ignored "-Wreturn-type"
#endif
#include <gtest/gtest.h>
#include <CPython/ConvertDatatypes.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Field.h>
#include <CPython/validatePython.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>



#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION
#include <numpy/arrayobject.h>
#include <iostream>
#include <memory>
#include <vector>
using namespace DB;


TEST(PythonConvertDatatypes, PythonListAndColumnConversion)
{
    setenv("PYTHONPATH", "/usr/lib/python3.10", 1);
    setenv("PYTHONHOME", "/usr/lib/python3.10", 1);
    Py_Initialize();

    // /**
    //  * test for column to python list conversion, first convert column to python list, then convert python list to column,
    //  * second convert the python list to column, and compare the result column with the original column
    // */
    {
    #define GENERATE_TEST(COLUMN_TYPE, CONVERTOR, DATATYPE) \
        { \
            auto col = COLUMN_TYPE::create(); \
            col->getData().push_back(1); \
            col->getData().push_back(2); \
            col->getData().push_back(3); \
            PyObject * py_list = cpython::convertColumnToPythonList(*col, std::make_shared<DATATYPE>(), 0, col->size()); \
            ASSERT_TRUE(py_list != nullptr); \
            ASSERT_TRUE(PyList_Check(py_list)); \
            ASSERT_EQ(PyList_Size(py_list), 3); \
            for (size_t i = 0; i < 3; i++) { \
                PyObject * item = PyList_GetItem(py_list, i); \
                ASSERT_EQ(CONVERTOR(item), i + 1); \
            } \
            auto res_col = cpython::convertPythonListToColumn(py_list, std::make_shared<DATATYPE>()); \
            ASSERT_TRUE(res_col != nullptr); \
            auto & col1 = assert_cast<const COLUMN_TYPE &>(*res_col); \
            auto & col2 = assert_cast<const COLUMN_TYPE &>(*col); \
            ASSERT_TRUE(col1.getData() == col2.getData()); \
        }
    
        GENERATE_TEST(ColumnUInt8, PyLong_AsUnsignedLong, DataTypeUInt8)
        GENERATE_TEST(ColumnUInt16, PyLong_AsUnsignedLong, DataTypeUInt16)
        GENERATE_TEST(ColumnUInt32, PyLong_AsUnsignedLong, DataTypeUInt32)
        GENERATE_TEST(ColumnUInt64, PyLong_AsUnsignedLong, DataTypeUInt64)
        GENERATE_TEST(ColumnInt8, PyLong_AsLong, DataTypeInt8)
        GENERATE_TEST(ColumnInt16, PyLong_AsLong, DataTypeInt16)
        GENERATE_TEST(ColumnInt32, PyLong_AsLong, DataTypeInt32)
        GENERATE_TEST(ColumnInt64, PyLong_AsLong, DataTypeInt64)
    }

    {
        // test for float32
        auto col = ColumnFloat32::create();
        col->getData().push_back(Float32(1.0));
        col->getData().push_back(Float32(2.0));
        col->getData().push_back(Float32(3.0));
        PyObject * py_list = cpython::convertColumnToPythonList(*col, std::make_shared<DataTypeFloat32>(), 0, col->size());
        ASSERT_TRUE(py_list != nullptr);
        ASSERT_TRUE(PyList_Check(py_list));
        ASSERT_EQ(PyList_Size(py_list), 3);
        for (size_t i = 0; i < 3; i++) {
            PyObject * item = PyList_GetItem(py_list, i);
            ASSERT_FLOAT_EQ(PyFloat_AsDouble(item), i + 1.0);
        }

        // convert python list to column
        auto col_ = cpython::convertPythonListToColumn(py_list, std::make_shared<DataTypeFloat32>());
        auto & col_res = assert_cast<const ColumnFloat32 &>(*col_);
        ASSERT_EQ(col_res.size(), 3);
        ASSERT_EQ(col_res.getData()[0], 1.0);
        ASSERT_EQ(col_res.getData()[1], 2.0);
        ASSERT_EQ(col_res.getData()[2], 3.0);
    }

    {
        // test for float64
        auto col = ColumnFloat64::create();
        col->getData().push_back(Float64(1.0));
        col->getData().push_back(Float64(2.0));
        col->getData().push_back(Float64(3.0));
        PyObject * py_list = cpython::convertColumnToPythonList(*col, std::make_shared<DataTypeFloat64>(), 0, col->size());
        ASSERT_TRUE(py_list != nullptr);
        ASSERT_TRUE(PyList_Check(py_list));
        ASSERT_EQ(PyList_Size(py_list), 3);
        for (size_t i = 0; i < 3; i++) {
            PyObject * item = PyList_GetItem(py_list, i);
            ASSERT_FLOAT_EQ(PyFloat_AsDouble(item), i + 1.0);
        }

        // convert python list to column
        auto col_ = cpython::convertPythonListToColumn(py_list, std::make_shared<DataTypeFloat64>());
        auto & col_res = assert_cast<const ColumnFloat64 &>(*col_);
        ASSERT_EQ(col_res.size(), 3);
        ASSERT_EQ(col_res.getData()[0], 1.0);
        ASSERT_EQ(col_res.getData()[1], 2.0);
        ASSERT_EQ(col_res.getData()[2], 3.0);
    }
    {
        // test for column string to list
        auto col = ColumnString::create();
        col->insert("hello");
        col->insert("world");
        col->insert("from");
        col->insert("python");
        PyObject * py_list = cpython::convertColumnToPythonList(*col, std::make_shared<DataTypeString>(), 0, col->size());
        ASSERT_TRUE(py_list != nullptr);
        ASSERT_TRUE(PyList_Check(py_list));
        ASSERT_EQ(PyList_Size(py_list), 4);
        for (size_t i = 0; i < 4; i++) {
            PyObject * item = PyList_GetItem(py_list, i);
            std::string str(PyUnicode_AsUTF8(item));
            ASSERT_EQ(str, col->getDataAt(i).toString());
        }

        auto col_ = cpython::convertPythonListToColumn(py_list, std::make_shared<DataTypeString>());
        auto & col_res = assert_cast<const ColumnString &>(*col_);
        ASSERT_EQ(col_res.size(), 4);
        ASSERT_EQ(col_res.getDataAt(0).toString(), "hello");
        ASSERT_EQ(col_res.getDataAt(1).toString(), "world");
        ASSERT_EQ(col_res.getDataAt(2).toString(), "from");
        ASSERT_EQ(col_res.getDataAt(3).toString(), "python");
    }
    {
        // test for column fixed string to list
        auto col = ColumnFixedString::create(16);
        col->insert("hello");
        col->insert("world");
        col->insert("from");
        col->insert("python");
        PyObject * py_list = cpython::convertColumnToPythonList(*col, std::make_shared<DataTypeFixedString>(16), 0, col->size());
        ASSERT_TRUE(py_list != nullptr);
        ASSERT_TRUE(PyList_Check(py_list));
        ASSERT_EQ(PyList_Size(py_list), 4);
        for (size_t i = 0; i < 4; i++) {
            PyObject * item = PyList_GetItem(py_list, i);
            std::string str(PyUnicode_AsUTF8(item));
            std::string col_str = col->getDataAt(i).toString();
            col_str.erase(std::find(col_str.begin(), col_str.end(), '\0'), col_str.end());
            ASSERT_EQ(str, col_str);
        }

        auto col_ = cpython::convertPythonListToColumn(py_list, std::make_shared<DataTypeFixedString>(16));
        auto & col_res = assert_cast<const ColumnFixedString &>(*col_);
        ASSERT_EQ(col_res.size(), 4);
        std::string str1 = col_res.getDataAt(0).toString();
        str1 = str1.substr(0, str1.find_first_of('\0'));
        ASSERT_EQ(str1, "hello");

        std::string str2 = col_res.getDataAt(1).toString();
        str2 = str2.substr(0, str2.find_first_of('\0'));
        ASSERT_EQ(str2, "world");

        std::string str3 = col_res.getDataAt(2).toString();
        str3 = str3.substr(0, str3.find_first_of('\0'));
        ASSERT_EQ(str3, "from");

        std::string str4 = col_res.getDataAt(3).toString();
        str4 = str4.substr(0, str4.find_first_of('\0'));
        ASSERT_EQ(str4, "python");
    }
    {
        // test for column array to list
        auto col = ColumnArray::create(ColumnUInt32::create());
        Array arr1;
        arr1.emplace_back(UInt32(1));
        arr1.emplace_back(UInt32(2));
        arr1.emplace_back(UInt32(3));

        Array arr2;
        arr2.emplace_back(UInt32(4));
        arr2.emplace_back(UInt32(5));
        arr2.emplace_back(UInt32(6));
        arr2.emplace_back(UInt32(7));

        col->insert(arr1);
        col->insert(arr2);

        // [[1, 2, 3], [4, 5, 6, 7]]
        PyObject * py_list = cpython::convertColumnToPythonList(*col, std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt32>()), 0, col->size());
        ASSERT_TRUE(py_list != nullptr);
        ASSERT_TRUE(PyList_Check(py_list));
        ASSERT_EQ(PyList_Size(py_list), 2);
        PyObject * item1 = PyList_GetItem(py_list, 0);
        PyObject * item2 = PyList_GetItem(py_list, 1);
        ASSERT_TRUE(PyList_Check(item1));
        ASSERT_TRUE(PyList_Check(item2));
        ASSERT_EQ(PyList_Size(item1), 3);
        ASSERT_EQ(PyList_Size(item2), 4);
        for (size_t i = 0; i < 3; i++) {
            PyObject * item = PyList_GetItem(item1, i);
            ASSERT_EQ(PyLong_AsUnsignedLong(item), i + 1);
        }
        for (size_t i = 0; i < 4; i++) {
            PyObject * item = PyList_GetItem(item2, i);
            ASSERT_EQ(PyLong_AsUnsignedLong(item), i + 4);
        }
        Py_DECREF(py_list);

    }
    {
        // test for tuple column to list
        // Columns columns;
        // columns.push_back(ColumnUInt32::create());
        // columns.push_back(ColumnString::create());
        // auto col = ColumnTuple::create(std::move(columns));

        // auto col_tuple = col->cloneEmpty();
        // Tuple tuple1;
        // tuple1.push_back(UInt32(1));
        // tuple1.push_back("hello");

        // Tuple tuple2;
        // tuple2.push_back(UInt32(2));
        // tuple2.push_back("world");

        // col_tuple->insert(tuple1);
        // col_tuple->insert(tuple2);

        // // [(1, 'hello'), (2, 'world')]
        // DataTypes tuple_data_types;
        // tuple_data_types.push_back(std::make_shared<DataTypeUInt32>());
        // tuple_data_types.push_back(std::make_shared<DataTypeString>());

        // auto tuple_data_type = std::make_shared<DataTypeTuple>(tuple_data_types);

        // const auto & col_ = assert_cast<const ColumnTuple &>(*col_tuple);
        // const auto & col_uint32 = assert_cast<const ColumnUInt32 &>(col_.getColumn(0));
        // const auto & col_str = assert_cast<const ColumnString &>(col_.getColumn(1));
        // PyObject * py_list = cpython::convertColumnToPythonList(*col_tuple, tuple_data_type, 0, col_tuple->size());
        // ASSERT_TRUE(py_list != nullptr);
        // ASSERT_TRUE(PyList_Check(py_list));
        // ASSERT_EQ(PyList_Size(py_list), 2);
        // PyObject * item1 = PyList_GetItem(py_list, 0);
        // ASSERT_TRUE(item1 != nullptr);
        // PyObject * item2 = PyList_GetItem(py_list, 1);
        // ASSERT_TRUE(item2 != nullptr);
        // ASSERT_TRUE(PyTuple_Check(item1));
        // ASSERT_TRUE(PyTuple_Check(item2));
        // ASSERT_EQ(PyTuple_Size(item1), 2);
        // ASSERT_EQ(PyTuple_Size(item2), 2);
        // PyObject * item1_1 = PyTuple_GetItem(item1, 0);
        // PyObject * item1_2 = PyTuple_GetItem(item1, 1);
        // PyObject * item2_1 = PyTuple_GetItem(item2, 0);
        // PyObject * item2_2 = PyTuple_GetItem(item2, 1);
        // ASSERT_EQ(PyLong_AsUnsignedLong(item1_1), col_uint32.getData()[0]);
        // auto str1(PyUnicode_AsUTF8(item1_2));
        // ASSERT_EQ(str1, col_str.getDataAt(0).toString());
        // ASSERT_EQ(PyLong_AsUnsignedLong(item2_1), col_uint32.getData()[1]);
        // auto str2(PyUnicode_AsUTF8(item2_2));
        // ASSERT_EQ(str2, col_str.getDataAt(1).toString());
        // Py_DECREF(py_list);
    }

    Py_Finalize();
}
TEST(PythonConvertDatatypes, CompicatedType)
{
    setenv("PYTHONPATH", "/usr/lib/python3.10", 1);
    setenv("PYTHONHOME", "/usr/lib/python3.10", 1);
    Py_Initialize();
    
    // {

    //     // test for column array to list
    //     auto col = ColumnArray::create(ColumnUInt32::create());
    //     Array arr1;
    //     arr1.emplace_back(UInt32(1));
    //     arr1.emplace_back(UInt32(2));
    //     arr1.emplace_back(UInt32(3));

    //     Array arr2;
    //     arr2.emplace_back(UInt32(4));
    //     arr2.emplace_back(UInt32(5));
    //     arr2.emplace_back(UInt32(6));
    //     arr2.emplace_back(UInt32(7));

    //     col->insert(arr1);
    //     col->insert(arr2);

    //     // [[1, 2, 3], [4, 5, 6, 7]]
    //     PyObject * py_list = cpython::convertColumnToPythonList(*col, std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt32>()), 0, col->size());
    //     ASSERT_TRUE(py_list != nullptr);
    //     ASSERT_TRUE(PyList_Check(py_list));
    //     ASSERT_EQ(PyList_Size(py_list), 2);
    //     PyObject * item1 = PyList_GetItem(py_list, 0);
    //     PyObject * item2 = PyList_GetItem(py_list, 1);
    //     ASSERT_TRUE(PyList_Check(item1));
    //     ASSERT_TRUE(PyList_Check(item2));
    //     ASSERT_EQ(PyList_Size(item1), 3);
    //     ASSERT_EQ(PyList_Size(item2), 4);
    //     for (size_t i = 0; i < 3; i++) {
    //         PyObject * item = PyList_GetItem(item1, i);
    //         ASSERT_EQ(PyLong_AsUnsignedLong(item), i + 1);
    //     }
    //     for (size_t i = 0; i < 4; i++) {
    //         PyObject * item = PyList_GetItem(item2, i);
    //         ASSERT_EQ(PyLong_AsUnsignedLong(item), i + 4);
    //     }

    //     std::cout << "python list adress1 " << py_list << std::endl;
    //     /// py_list: [[1, 2， 3], [4, 5， 6, 7]]
    //     auto column = cpython::convertPythonListToColumn(py_list, std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt32>()));
    //     const auto & col_array = assert_cast<const ColumnArray &>(*column);
    //     ASSERT_EQ(col_array.size(), 2);
    //     ASSERT_EQ(3, col_array.getOffsets()[0]);
    //     ASSERT_EQ(7, col_array.getOffsets()[1]);
    //     auto str = cpython::catchException();
    //     std::cout << "exception " << str << std::endl;
    // }
    {
        // test for tuple column to list
        Columns columns;
        columns.push_back(ColumnUInt32::create());
        columns.push_back(ColumnString::create());
        auto col = ColumnTuple::create(std::move(columns));

        auto col_tuple = col->cloneEmpty();
        Tuple tuple1;
        tuple1.push_back(UInt32(1));
        tuple1.push_back("hello");

        Tuple tuple2;
        tuple2.push_back(UInt32(2));
        tuple2.push_back("world");

        col_tuple->insert(tuple1);
        col_tuple->insert(tuple2);

        // [(1, 'hello'), (2, 'world')]
        DataTypes tuple_data_types;
        tuple_data_types.push_back(std::make_shared<DataTypeUInt32>());
        tuple_data_types.push_back(std::make_shared<DataTypeString>());

        auto tuple_data_type = std::make_shared<DataTypeTuple>(tuple_data_types);

        const auto & col_ = assert_cast<const ColumnTuple &>(*col_tuple);
        const auto & col_uint32 = assert_cast<const ColumnUInt32 &>(col_.getColumn(0));
        const auto & col_str = assert_cast<const ColumnString &>(col_.getColumn(1));
        PyObject * py_list = cpython::convertColumnToPythonList(*col_tuple, tuple_data_type, 0, col_tuple->size());
        ASSERT_TRUE(py_list != nullptr);
        ASSERT_TRUE(PyList_Check(py_list));
        ASSERT_EQ(PyList_Size(py_list), 2);
        PyObject * item1 = PyList_GetItem(py_list, 0);
        ASSERT_TRUE(item1 != nullptr);
        PyObject * item2 = PyList_GetItem(py_list, 1);
        ASSERT_TRUE(item2 != nullptr);
        ASSERT_TRUE(PyTuple_Check(item1));
        ASSERT_TRUE(PyTuple_Check(item2));
        ASSERT_EQ(PyTuple_Size(item1), 2);
        ASSERT_EQ(PyTuple_Size(item2), 2);
        PyObject * item1_1 = PyTuple_GetItem(item1, 0);
        PyObject * item1_2 = PyTuple_GetItem(item1, 1);
        PyObject * item2_1 = PyTuple_GetItem(item2, 0);
        PyObject * item2_2 = PyTuple_GetItem(item2, 1);
        ASSERT_EQ(PyLong_AsUnsignedLong(item1_1), col_uint32.getData()[0]);
        auto str1(PyUnicode_AsUTF8(item1_2));
        ASSERT_EQ(str1, col_str.getDataAt(0).toString());
        ASSERT_EQ(PyLong_AsUnsignedLong(item2_1), col_uint32.getData()[1]);
        auto str2(PyUnicode_AsUTF8(item2_2));
        ASSERT_EQ(str2, col_str.getDataAt(1).toString());

        /// list(tuple): [(1, 'hello'), (2, 'world')] -> tuple column
        auto tuple_data_type_2 = std::make_shared<DataTypeTuple>(tuple_data_types);
        auto column = cpython::convertPythonListToColumn(py_list, tuple_data_type_2);

        const auto & col_tuple_2 = assert_cast<const ColumnTuple &>(*column);
        const auto & col_uint32_2 = assert_cast<const ColumnUInt32 &>(col_tuple_2.getColumn(0));
        const auto & col_str_2 = assert_cast<const ColumnString &>(col_tuple_2.getColumn(1));
        ASSERT_EQ(col_tuple_2.size(), 2);
        ASSERT_EQ(col_uint32_2.getData()[0], 1);
        ASSERT_EQ(col_uint32_2.getData()[1], 2);
        ASSERT_EQ(col_str_2.getDataAt(0).toString(), "hello");
        ASSERT_EQ(col_str_2.getDataAt(1).toString(), "world");
    }
    // {
    //     // test for python map and map column
    //     Array pair1;
    //     pair1.emplace_back(UInt32(1));
    //     pair1.emplace_back("hello");

    //     Array pair2;
    //     pair2.emplace_back(UInt32(2));
    //     pair2.emplace_back("world");

    //     Map map;
    //     map.emplace_back(pair1);
    //     map.emplace_back(pair2);

    //     auto col = ColumnMap::create(ColumnArray::create(ColumnTuple::create({ColumnUInt32::create(), ColumnString::create()})));
    //     auto & col_map = assert_cast<ColumnMap &>(*col);
    //     col_map.insert(map);

    //     ASSERT_EQ(col_map.size(), 2);

    // }
    Py_Finalize();
}

bool init_numpy() {
    import_array1(false)
    return true;
}


template<typename T>
std::pair<T *, npy_intp> getInternalDataAndSize(PyObject * np_array) {
    PyArrayObject* numpy_array = reinterpret_cast<PyArrayObject*>(np_array);
    T * data = static_cast<T *>(PyArray_DATA(numpy_array));
    npy_intp size = PyArray_SIZE(numpy_array);
    return std::make_pair(data, size);
}



TEST(PythonConvertDatatypes, columnAndNumpyConversion)
{
    setenv("PYTHONPATH", "/usr/lib/python3.10", 1);
    setenv("PYTHONHOME", "/usr/lib/python3.10", 1);
    Py_Initialize();
    ASSERT_TRUE(init_numpy());
#define GENERATE_TEST_1(TYPE, COLUMN_TYPE, DATA_TYPE) \
    { \
        auto col = COLUMN_TYPE::create(); \
        col->getData().push_back(1); \
        col->getData().push_back(2); \
        col->getData().push_back(3); \
        auto * address = col->getData().data(); \
        ColumnWithTypeAndName column_with_type(std::move(col), std::make_shared<DATA_TYPE>(), "column"); \
        PyObject * np_array = cpython::convertColumnToNumpyArray(column_with_type); \
        ASSERT_TRUE(np_array != nullptr); \
        auto [data, size] = getInternalDataAndSize<TYPE>(np_array); \
        ASSERT_EQ(size, 3); \
        ASSERT_EQ(data[0], 1); \
        ASSERT_EQ(data[1], 2); \
        ASSERT_EQ(data[2], 3); \
        /* check if the addres is the same(zero copy)*/  \
        ASSERT_EQ(data, address); \
        Py_DECREF(np_array); \
    }
    {
        GENERATE_TEST_1(UInt8, ColumnUInt8, DataTypeUInt8)
        GENERATE_TEST_1(UInt16, ColumnUInt16, DataTypeUInt16)
        GENERATE_TEST_1(UInt32, ColumnUInt32, DataTypeUInt32)
        GENERATE_TEST_1(UInt64, ColumnUInt64, DataTypeUInt64)
        GENERATE_TEST_1(Int8, ColumnInt8, DataTypeInt8)
        GENERATE_TEST_1(Int16, ColumnInt16, DataTypeInt16)
        GENERATE_TEST_1(Int32, ColumnInt32, DataTypeInt32)
        GENERATE_TEST_1(Int64, ColumnInt64, DataTypeInt64)
    }

    {
        // test for float32
        auto col = ColumnFloat32::create();
        col->getData().push_back(Float32(1.0));
        col->getData().push_back(Float32(2.0));
        col->getData().push_back(Float32(3.0));
        auto * address = col->getData().data();
        ColumnWithTypeAndName column_with_type(std::move(col), std::make_shared<DataTypeFloat32>(), "column");
        PyObject * np_array = cpython::convertColumnToNumpyArray(column_with_type);
        ASSERT_TRUE(np_array != nullptr);
        auto [data, size] = getInternalDataAndSize<Float32>(np_array);
        ASSERT_EQ(size, 3);
        ASSERT_FLOAT_EQ(data[0], 1.0);
        ASSERT_FLOAT_EQ(data[1], 2.0);
        ASSERT_FLOAT_EQ(data[2], 3.0);
        // check if the addres is the same(zero copy)
        ASSERT_EQ(data, address);
        Py_DECREF(np_array);
    }
    {
        // test for float64
        auto col = ColumnFloat64::create();
        col->getData().push_back(Float64(1.0));
        col->getData().push_back(Float64(2.0));
        col->getData().push_back(Float64(3.0));
        auto * address = col->getData().data();
        ColumnWithTypeAndName column_with_type(std::move(col), std::make_shared<DataTypeFloat64>(), "column");
        PyObject * np_array = cpython::convertColumnToNumpyArray(column_with_type);
        ASSERT_TRUE(np_array != nullptr);
        auto [data, size] = getInternalDataAndSize<Float64>(np_array);
        ASSERT_EQ(size, 3);
        ASSERT_FLOAT_EQ(data[0], 1.0);
        ASSERT_FLOAT_EQ(data[1], 2.0);
        ASSERT_FLOAT_EQ(data[2], 3.0);
        // check if the addres is the same(zero copy)
        ASSERT_EQ(data, address);
        Py_DECREF(np_array);
    }

    {
        // test for type string
        auto col = ColumnString::create();
        col->insert("hello");
        col->insert("world");
        col->insert("from");
        col->insert("numpy");
        ColumnWithTypeAndName column_with_type(std::move(col), std::make_shared<DataTypeString>(), "column");
        PyObject * np_array = cpython::convertColumnToNumpyArray(column_with_type);
        PyArrayObject *numpy_array = reinterpret_cast<PyArrayObject*>(np_array);
        PyObject ** result_data = static_cast<PyObject **>(PyArray_DATA(numpy_array));
        npy_intp size = PyArray_SIZE(numpy_array);
        ASSERT_EQ(size, column_with_type.column->size());
        for (npy_intp i = 0; i < size; i++) {
            PyObject * item = result_data[i];
            std::string str(PyUnicode_AsUTF8(item));
            ASSERT_EQ(str, column_with_type.column->getDataAt(i).toString());
        }
        Py_DECREF(np_array);
    }

    {
        // test for fixed string
        auto col = ColumnFixedString::create(16);
        col->insert("hello");
        col->insert("world");
        col->insert("from");
        col->insert("numpy");
        ColumnWithTypeAndName column_with_type(std::move(col), std::make_shared<DataTypeFixedString>(16), "column");
        PyObject * np_array = cpython::convertColumnToNumpyArray(column_with_type);
        PyArrayObject *numpy_array = reinterpret_cast<PyArrayObject*>(np_array);
        int type = PyArray_TYPE(numpy_array);
        npy_intp size = PyArray_SIZE(numpy_array);
        ASSERT_EQ(size, column_with_type.column->size());

        // type of numpy array element is NPY_STRING
        ASSERT_EQ(type, NPY_STRING);
        char **result_data = reinterpret_cast<char **>(PyArray_DATA(numpy_array));        
        const auto & column_fixed = assert_cast<const ColumnFixedString &>(*(column_with_type.column));
        // check address
        auto result_data_ptr = reinterpret_cast<const char8_t *>(result_data);
        ASSERT_EQ(result_data_ptr, column_fixed.getChars().data());

        for (npy_intp i = 0; i < size; i++) {
            auto res = memcmp(result_data_ptr + i * 16, column_fixed.getDataAt(i).data, 16);
            ASSERT_EQ(res, 0);
        }
        Py_DECREF(np_array);
    }

    // convert numpy array to column test
#define GENERATE_TEST_2(COLUMN_TYPE, DATA_TYPE) \
    { \
        auto col = COLUMN_TYPE::create(); \
        col->getData().push_back(1); \
        col->getData().push_back(2); \
        col->getData().push_back(3); \
        ColumnWithTypeAndName column_with_type(std::move(col), std::make_shared<DATA_TYPE>(), "column"); \
        PyObject * np_array = cpython::convertColumnToNumpyArray(column_with_type); \
        ASSERT_TRUE(np_array != nullptr); \
        auto result = cpython::covertNumpyArrayToColumn(np_array, column_with_type.type); \
        ASSERT_TRUE(result != nullptr); \
        ASSERT_EQ(result->size(), 3); \
        auto & col_result = assert_cast<const COLUMN_TYPE &>(*result); \
        ASSERT_EQ(col_result.getData()[0], 1); \
        ASSERT_EQ(col_result.getData()[1], 2); \
        ASSERT_EQ(col_result.getData()[2], 3); \
        Py_DECREF(np_array); \
    }
    {
        GENERATE_TEST_2(ColumnUInt8, DataTypeUInt8)
        GENERATE_TEST_2(ColumnUInt16, DataTypeUInt16)
        GENERATE_TEST_2(ColumnUInt32, DataTypeUInt32)
        GENERATE_TEST_2(ColumnUInt64, DataTypeUInt64)
        GENERATE_TEST_2(ColumnInt8, DataTypeInt8)
        GENERATE_TEST_2(ColumnInt16, DataTypeInt16)
        GENERATE_TEST_2(ColumnInt32, DataTypeInt32)
        GENERATE_TEST_2(ColumnInt64, DataTypeInt64)
    }


    Py_Finalize();
}

TEST(PythonConvertDatatypes, columntest)
{
    // [[maybe_unused]] auto col = ColumnArray::create(ColumnUInt32::create());
    // auto col_ = IColumn::mutate(std::move(col));
    // auto & col_arr = assert_cast<ColumnArray &>(*col_);

    // Array arr1;
    // arr1.emplace_back(UInt32(1));
    // arr1.emplace_back(UInt32(2));

    // Array arr2;
    // arr2.emplace_back(UInt32(3));
    // arr2.emplace_back(UInt32(4));
    // arr2.emplace_back(UInt32(5));

    // col_arr.insert(arr1);
    // col_arr.insert(arr2);

    // std::cout << col_arr.getOffsets()[0] << std::endl;
    // std::cout << col_arr.getOffsets()[1] << std::endl;
    


    // auto col_uint32 = ColumnUInt32::create();
    // col_uint32->getData().push_back(1);
    // col_uint32->getData().push_back(2);
    // col_uint32->getData().push_back(3);
    // col_arr.getData().insertRangeFrom(*col_uint32, 0, col_uint32->size());
    // col_arr.getOffsets().push_back(3);

    // auto col_uint2 = ColumnUInt32::create();
    // col_uint2->getData().push_back(4);
    // col_uint2->getData().push_back(5);
    // col_arr.getData().insertRangeFrom(*col_uint2, 0, col_uint2->size());
    // col_arr.getOffsets().push_back(2);

    // [[maybe_unused]] int a = 0;
    // ASSERT_EQ(col_arr.size(), 2);

}
#endif