#include "config.h"

#ifdef HAS_RESERVED_IDENTIFIER
#pragma clang diagnostic ignored "-Wreturn-type"
#endif

#if USE_PYTHON_UDF

#include <CPython/ConvertDatatypes.h>
#include <CPython/tests/CPythonTest.h>
#include <CPython/validatePython.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Common/Exception.h>
#include <Common/LocalDate.h>
#include <Common/LocalDateTime.h>
#include <Common/LocalDateTime64.h>

#include <datetime.h>

#if USE_NUMPY
#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION
#include <numpy/arrayobject.h>
#endif

#include <memory>
#include <vector>
using namespace DB;

namespace
{
DataTypePtr makeDataType(const String & full_name)
{
    return DataTypeFactory::instance().get(full_name);
}

std::string pythonBytesAsString(PyObject * py_bytes)
{
    if (!py_bytes || !PyBytes_Check(py_bytes))
        return "";

    char * buffer = nullptr;
    Py_ssize_t length = 0;

    if (PyBytes_AsStringAndSize(py_bytes, &buffer, &length) < 0)
        return "";

    return std::string(buffer, length);
}

template <typename ColumnType, typename T>
void testColumnConvertion(const std::vector<T> & data, String datatype, std::function<void(PyObject *, const std::decay_t<T> &)> checker)
{
    auto data_type = makeDataType(datatype);
    auto col = data_type->createColumn();
    for (const auto & elem : data)
        col->insert(Field(elem));

    auto py_list = cpython::convertColumnToPythonList(*col, data_type, 0, col->size());
    ASSERT_TRUE(py_list);
    ASSERT_TRUE(PyList_Check(py_list.get()));
    ASSERT_EQ(PyList_Size(py_list.get()), data.size());
    for (size_t i = 0; i < data.size(); i++)
    {
        /// No need to wrap the item in PyObjectPtr, as it is borrowed reference
        /// it won't increase the reference count so we don't need to decrease it
        PyObject * item = PyList_GetItem(py_list.get(), i);
        checker(item, data[i]);
    }

    auto res_col = cpython::convertPythonListToColumn(py_list, data_type);
    ASSERT_TRUE(res_col != nullptr);
    auto & col1 = assert_cast<const ColumnType &>(*res_col);
    auto & col2 = assert_cast<const ColumnType &>(*col);
    ASSERT_TRUE(col1.size() == col2.size());

    for (size_t i = 0; i < col1.size(); i++)
        ASSERT_TRUE(col1[i] == col2[i]);
}
}

TEST_F(CPythonTest, ColumnUInt8)
{
    assertNoLeak([]() {
        testColumnConvertion<ColumnUInt8>(std::vector<UInt8>{1, 2, 3}, "uint8", [](PyObject * item, const UInt8 & v) {
            ASSERT_TRUE(PyLong_Check(item));
            ASSERT_EQ(PyLong_AsUnsignedLong(item), v);
        });
    });
}

TEST_F(CPythonTest, ColumnUInt16)
{
    assertNoLeak([]() {
        testColumnConvertion<ColumnUInt16>(std::vector<UInt16>{1, 2, 3}, "uint16", [](PyObject * item, const UInt16 & v) {
            ASSERT_TRUE(PyLong_Check(item));
            ASSERT_EQ(PyLong_AsUnsignedLong(item), v);
        });
    });
}

TEST_F(CPythonTest, ColumnUInt32)
{
    assertNoLeak([]() {
        testColumnConvertion<ColumnUInt32>(std::vector<UInt32>{1, 2, 3}, "uint32", [](PyObject * item, const UInt32 & v) {
            ASSERT_TRUE(PyLong_Check(item));
            ASSERT_EQ(PyLong_AsUnsignedLong(item), v);
        });
    });
}

TEST_F(CPythonTest, ColumnUInt64)
{
    assertNoLeak([]() {
        testColumnConvertion<ColumnUInt64>(std::vector<UInt64>{1, 2, 3}, "uint64", [](PyObject * item, const UInt64 & v) {
            ASSERT_TRUE(PyLong_Check(item));
            ASSERT_EQ(PyLong_AsUnsignedLong(item), v);
        });
    });
}

TEST_F(CPythonTest, ColumnInt8)
{
    assertNoLeak([]() {
        testColumnConvertion<ColumnInt8>(std::vector<Int8>{-1, 2, 3}, "int8", [](PyObject * item, const Int8 & v) {
            ASSERT_TRUE(PyLong_Check(item));
            ASSERT_EQ(PyLong_AsLong(item), v);
        });
    });
}

TEST_F(CPythonTest, ColumnInt16)
{
    assertNoLeak([]() {
        testColumnConvertion<ColumnInt16>(std::vector<Int16>{-1, 2, 3}, "int16", [](PyObject * item, const Int16 & v) {
            ASSERT_TRUE(PyLong_Check(item));
            ASSERT_EQ(PyLong_AsLong(item), v);
        });
    });
}

TEST_F(CPythonTest, ColumnInt32)
{
    assertNoLeak([]() {
        testColumnConvertion<ColumnInt32>(std::vector<Int32>{-1, 2, 3}, "int32", [](PyObject * item, const Int32 & v) {
            ASSERT_TRUE(PyLong_Check(item));
            ASSERT_EQ(PyLong_AsLong(item), v);
        });
    });
}

TEST_F(CPythonTest, ColumnInt64)
{
    assertNoLeak([]() {
        testColumnConvertion<ColumnInt64>(std::vector<Int64>{-1, 2, 3}, "int64", [](PyObject * item, const Int64 & v) {
            ASSERT_TRUE(PyLong_Check(item));
            ASSERT_EQ(PyLong_AsLong(item), v);
        });
    });
}

#if USE_NUMPY

TEST_F(CPythonTest, ColumnInt64FromNumpyScalar)
{
    bool has_numpy = true;

    assertNoLeak([&]() {
        cpython::PyObjectPtr numpy{PyImport_ImportModule("numpy")};
        if (!numpy)
        {
            if (PyErr_Occurred())
                PyErr_Clear();
            has_numpy = false;
            return;
        }

        cpython::PyObjectPtr int64_type{PyObject_GetAttrString(numpy.get(), "int64")};
        ASSERT_TRUE(int64_type);

        cpython::PyObjectPtr arg{PyLong_FromLongLong(15)};
        ASSERT_TRUE(arg);

        cpython::PyObjectPtr scalar{PyObject_CallFunctionObjArgs(int64_type.get(), arg.get(), nullptr)};
        ASSERT_TRUE(scalar);

        auto data_type = makeDataType("int64");
        auto column = data_type->createColumn();
        cpython::insertPythonObjectToColumn(*column, scalar, data_type);

        auto & col = assert_cast<ColumnInt64 &>(*column);
        ASSERT_EQ(col.size(), 1);
        ASSERT_EQ(col[0], 15);
    });

    if (!has_numpy)
        GTEST_SKIP() << "numpy is not available in the test environment";
}

TEST_F(CPythonTest, ColumnUInt64FromNumpyScalar)
{
    bool has_numpy = true;

    assertNoLeak([&]() {
        cpython::PyObjectPtr numpy{PyImport_ImportModule("numpy")};
        if (!numpy)
        {
            if (PyErr_Occurred())
                PyErr_Clear();
            has_numpy = false;
            return;
        }

        cpython::PyObjectPtr uint64_type{PyObject_GetAttrString(numpy.get(), "uint64")};
        ASSERT_TRUE(uint64_type);

        cpython::PyObjectPtr arg{PyLong_FromLongLong(15)};
        ASSERT_TRUE(arg);

        cpython::PyObjectPtr scalar{PyObject_CallFunctionObjArgs(uint64_type.get(), arg.get(), nullptr)};
        ASSERT_TRUE(scalar);

        auto data_type = makeDataType("uint64");
        auto column = data_type->createColumn();
        cpython::insertPythonObjectToColumn(*column, scalar, data_type);

        auto & col = assert_cast<ColumnUInt64 &>(*column);
        ASSERT_EQ(col.size(), 1);
        ASSERT_EQ(col[0], 15);
    });

    if (!has_numpy)
        GTEST_SKIP() << "numpy is not available in the test environment";
}

#endif

TEST_F(CPythonTest, ColumnNullableInt32)
{
    assertNoLeak([]() {
        auto data_type = makeDataType("nullable(int32)");
        auto col = data_type->createColumn();

        col->insert(Field(Int32(1)));
        col->insert(Field{});
        col->insert(Field(Int32(-3)));

        auto py_list = cpython::convertColumnToPythonList(*col, data_type, 0, col->size());
        ASSERT_TRUE(py_list);
        ASSERT_TRUE(PyList_Check(py_list.get()));
        ASSERT_EQ(PyList_Size(py_list.get()), 3);

        PyObject * item0 = PyList_GetItem(py_list.get(), 0);
        ASSERT_TRUE(PyLong_Check(item0));
        ASSERT_EQ(PyLong_AsLong(item0), 1);

        PyObject * item1 = PyList_GetItem(py_list.get(), 1);
        ASSERT_TRUE(Py_IsNone(item1));

        PyObject * item2 = PyList_GetItem(py_list.get(), 2);
        ASSERT_TRUE(PyLong_Check(item2));
        ASSERT_EQ(PyLong_AsLong(item2), -3);

        auto res_col = cpython::convertPythonListToColumn(py_list, data_type);
        ASSERT_TRUE(res_col != nullptr);
        ASSERT_EQ(res_col->size(), col->size());
        for (size_t i = 0; i < res_col->size(); i++)
            ASSERT_TRUE((*res_col)[i] == (*col)[i]);
    });
}

TEST_F(CPythonTest, ColumnArrayNullableInt32)
{
    assertNoLeak([]() {
        auto data_type = makeDataType("array(nullable(int32))");
        auto col = data_type->createColumn();

        col->insert(Field(Array{Field(Int32(1)), Field{}, Field(Int32(3))}));
        col->insert(Field(Array{Field{}, Field(Int32(2))}));

        auto py_list = cpython::convertColumnToPythonList(*col, data_type, 0, col->size());
        ASSERT_TRUE(py_list);
        ASSERT_TRUE(PyList_Check(py_list.get()));
        ASSERT_EQ(PyList_Size(py_list.get()), 2);

        PyObject * row0 = PyList_GetItem(py_list.get(), 0);
        ASSERT_TRUE(PyList_Check(row0));
        ASSERT_EQ(PyList_Size(row0), 3);
        ASSERT_EQ(PyLong_AsLong(PyList_GetItem(row0, 0)), 1);
        ASSERT_TRUE(Py_IsNone(PyList_GetItem(row0, 1)));
        ASSERT_EQ(PyLong_AsLong(PyList_GetItem(row0, 2)), 3);

        PyObject * row1 = PyList_GetItem(py_list.get(), 1);
        ASSERT_TRUE(PyList_Check(row1));
        ASSERT_EQ(PyList_Size(row1), 2);
        ASSERT_TRUE(Py_IsNone(PyList_GetItem(row1, 0)));
        ASSERT_EQ(PyLong_AsLong(PyList_GetItem(row1, 1)), 2);

        auto res_col = cpython::convertPythonListToColumn(py_list, data_type);
        ASSERT_TRUE(res_col != nullptr);
        ASSERT_EQ(res_col->size(), col->size());
        for (size_t i = 0; i < res_col->size(); i++)
            ASSERT_TRUE((*res_col)[i] == (*col)[i]);
    });
}

TEST_F(CPythonTest, ColumnFloat32)
{
    assertNoLeak([]() {
        testColumnConvertion<ColumnFloat32>(std::vector<Float32>{-1.1f, 2.2f, 3.3f}, "float32", [](PyObject * item, const Float32 & v) {
            ASSERT_TRUE(PyFloat_Check(item));
            ASSERT_EQ(PyFloat_AsDouble(item), v);
        });
    });
}

TEST_F(CPythonTest, ColumnFloat64)
{
    assertNoLeak([]() {
        testColumnConvertion<ColumnFloat64>(std::vector<Float64>{-1.1, 2.2, 3.3}, "float64", [](PyObject * item, const Float64 & v) {
            ASSERT_TRUE(PyFloat_Check(item));
            ASSERT_EQ(PyFloat_AsDouble(item), v);
        });
    });
}

TEST_F(CPythonTest, ColumnDate)
{
    PyDateTime_IMPORT;

    assertNoLeak([]() {
        testColumnConvertion<ColumnDate>(
            std::vector<UInt16>{
                LocalDate(2024, 12, 12).getDayNum().toUnderType(),
                LocalDate(2024, 12, 13).getDayNum().toUnderType(),
                LocalDate(2024, 12, 14).getDayNum().toUnderType()},
            "date",
            [](PyObject * item, const UInt16 & v) {
                ASSERT_TRUE(PyDate_Check(item));

                LocalDate local_date{DayNum(v)};

                ASSERT_EQ(PyDateTime_GET_YEAR(item), local_date.year());
                ASSERT_EQ(PyDateTime_GET_MONTH(item), local_date.month());
                ASSERT_EQ(PyDateTime_GET_DAY(item), local_date.day());
            });
    });
}

TEST_F(CPythonTest, ColumnDate32)
{
    PyDateTime_IMPORT;

    assertNoLeak([]() {
        testColumnConvertion<ColumnDate32>(
            std::vector<Int32>{
                LocalDate(2024, 12, 12).getExtenedDayNum().toUnderType(),
                LocalDate(2024, 12, 13).getExtenedDayNum().toUnderType(),
                LocalDate(2024, 12, 14).getExtenedDayNum().toUnderType()},
            "date32",
            [](PyObject * item, const Int32 & v) {
                ASSERT_TRUE(PyDate_Check(item));

                LocalDate local_date{ExtendedDayNum(v)};

                ASSERT_EQ(PyDateTime_GET_YEAR(item), local_date.year());
                ASSERT_EQ(PyDateTime_GET_MONTH(item), local_date.month());
                ASSERT_EQ(PyDateTime_GET_DAY(item), local_date.day());
            });
    });
}

TEST_F(CPythonTest, ColumnDateTime)
{
    PyDateTime_IMPORT;

    assertNoLeak([]() {
        testColumnConvertion<ColumnDateTime>(
            std::vector<UInt32>{
                static_cast<UInt32>(LocalDateTime(2024, 12, 12, 1, 2, 3).to_time_t()),
                static_cast<UInt32>(LocalDateTime(2024, 12, 13, 1, 2, 3).to_time_t()),
                static_cast<UInt32>(LocalDateTime(2024, 12, 14, 1, 2, 3).to_time_t())},
            "datetime",
            [](PyObject * item, const UInt32 & v) {
                ASSERT_TRUE(PyDateTime_Check(item));

                LocalDateTime local_datetime{v};

                ASSERT_EQ(PyDateTime_GET_YEAR(item), local_datetime.year());
                ASSERT_EQ(PyDateTime_GET_MONTH(item), local_datetime.month());
                ASSERT_EQ(PyDateTime_GET_DAY(item), local_datetime.day());
                ASSERT_EQ(PyDateTime_DATE_GET_HOUR(item), local_datetime.hour());
                ASSERT_EQ(PyDateTime_DATE_GET_MINUTE(item), local_datetime.minute());
                ASSERT_EQ(PyDateTime_DATE_GET_SECOND(item), local_datetime.second());
            });
    });
}

TEST_F(CPythonTest, ColumnDateTime64)
{
    PyDateTime_IMPORT;

    auto to_datetime64 = [](const std::string & local_datetime) {
        ReadBufferFromString buf(local_datetime);
        DateTime64 datetime64;

        readDateTime64Text(datetime64, 3, buf);
        return datetime64;
    };

    std::vector<std::string> input_str = {
        "2024-12-12 01:02:03.123",
        "2024-12-13 01:02:03.123",
        "2024-12-14 01:02:03.123",
    };

    std::vector<DateTime64> input;
    input.reserve(input_str.size());

    for (const auto & s : input_str)
        input.push_back(to_datetime64(s));

    assertNoLeak([&]() {
        testColumnConvertion<ColumnDateTime64>(input, "datetime64(3)", [](PyObject * item, const DateTime64 & v) {
            ASSERT_TRUE(PyDateTime_Check(item));

            LocalDateTime64 local_datetime64(v, 3);

            ASSERT_EQ(PyDateTime_GET_YEAR(item), local_datetime64.year());
            ASSERT_EQ(PyDateTime_GET_MONTH(item), local_datetime64.month());
            ASSERT_EQ(PyDateTime_GET_DAY(item), local_datetime64.day());
            ASSERT_EQ(PyDateTime_DATE_GET_HOUR(item), local_datetime64.hour());
            ASSERT_EQ(PyDateTime_DATE_GET_MINUTE(item), local_datetime64.minute());
            ASSERT_EQ(PyDateTime_DATE_GET_SECOND(item), local_datetime64.second());
            ASSERT_EQ(PyDateTime_DATE_GET_MICROSECOND(item), local_datetime64.microsecond());
        });
    });
}

TEST_F(CPythonTest, ColumnString)
{
    assertNoLeak([]() {
        testColumnConvertion<ColumnString>(std::vector<String>{"hello", "world"}, "string", [](PyObject * item, const String & v) {
            ASSERT_TRUE(PyBytes_Check(item));
            ASSERT_EQ(pythonBytesAsString(item), v);
        });
    });
}

TEST_F(CPythonTest, ColumnFixedString)
{
    assertNoLeak([]() {
        testColumnConvertion<ColumnFixedString, String>(
            std::vector<String>{"hello", "world"}, "fixed_string(16)", [](PyObject * item, const String & v) {
                ASSERT_TRUE(PyBytes_Check(item));
                /// hello -> "hello\0\0\0\0\0\0\0\0\0\0\0" in fixed string with length 16
                ASSERT_EQ(String{pythonBytesAsString(item).c_str()}, v);
            });
    });
}

TEST_F(CPythonTest, ColumnArray)
{
    assertNoLeak([]() {
        Array arr1;
        arr1.emplace_back(UInt32(1));
        arr1.emplace_back(UInt32(2));
        arr1.emplace_back(UInt32(3));

        Array arr2;
        arr2.emplace_back(UInt32(4));
        arr2.emplace_back(UInt32(5));
        arr2.emplace_back(UInt32(6));
        arr2.emplace_back(UInt32(7));

        testColumnConvertion<ColumnArray, Array>(std::vector<Array>{arr1, arr2}, "array(uint32)", [](PyObject * item, const Array & v) {
            ASSERT_TRUE(PyList_Check(item));

            ASSERT_EQ(PyList_Size(item), v.size());

            for (size_t i = 0; i < v.size(); i++)
            {
                PyObject * elem = PyList_GetItem(item, i);
                ASSERT_TRUE(PyLong_Check(elem));
                ASSERT_EQ(PyLong_AsUnsignedLong(elem), v[i].get<UInt32>());
            }
        });
    });
}

TEST_F(CPythonTest, ColumnTuple)
{
    assertNoLeak([]() {
        Tuple tuple1;
        tuple1.push_back(UInt32(1));
        tuple1.push_back(String("hello"));

        Tuple tuple2;
        tuple2.push_back(UInt32(2));
        tuple2.push_back(String("world"));

        testColumnConvertion<ColumnTuple, Tuple>(
            std::vector<Tuple>{tuple1, tuple2}, "tuple(uint32, string)", [](PyObject * item, const Tuple & v) {
                ASSERT_TRUE(PyTuple_Check(item));

                ASSERT_EQ(PyTuple_Size(item), v.size());

                PyObject * first = PyTuple_GetItem(item, 0);
                ASSERT_TRUE(PyLong_Check(first));
                ASSERT_EQ(PyLong_AsUnsignedLong(first), v[0].get<UInt32>());

                PyObject * second = PyTuple_GetItem(item, 1);
                ASSERT_TRUE(PyBytes_Check(second));
                ASSERT_EQ(pythonBytesAsString(second), v[1].get<String>());
            });
    });
}

TEST_F(CPythonTest, ColumnTupleFromListRows)
{
    assertNoLeak([]() {
        auto data_type = makeDataType("tuple(uint32, string)");

        cpython::PyObjectPtr py_list{PyList_New(2)};
        ASSERT_TRUE(py_list);

        cpython::PyObjectPtr row0{PyList_New(2)};
        ASSERT_TRUE(row0);
        PyList_SET_ITEM(row0.get(), 0, PyLong_FromUnsignedLong(1));
        PyList_SET_ITEM(row0.get(), 1, PyUnicode_FromString("hello"));
        PyList_SET_ITEM(py_list.get(), 0, row0.release());

        cpython::PyObjectPtr row1{PyList_New(2)};
        ASSERT_TRUE(row1);
        PyList_SET_ITEM(row1.get(), 0, PyLong_FromUnsignedLong(2));
        PyList_SET_ITEM(row1.get(), 1, PyUnicode_FromString("world"));
        PyList_SET_ITEM(py_list.get(), 1, row1.release());

        auto expected = data_type->createColumn();
        Tuple tuple0;
        tuple0.push_back(UInt32(1));
        tuple0.push_back(String("hello"));
        expected->insert(tuple0);

        Tuple tuple1;
        tuple1.push_back(UInt32(2));
        tuple1.push_back(String("world"));
        expected->insert(tuple1);

        auto res_col = cpython::convertPythonListToColumn(py_list, data_type);
        ASSERT_TRUE(res_col != nullptr);
        ASSERT_EQ(res_col->size(), expected->size());
        for (size_t i = 0; i < expected->size(); i++)
            ASSERT_TRUE((*res_col)[i] == (*expected)[i]);
    });
}

TEST_F(CPythonTest, ColumnTupleRejectsWrongArity)
{
    assertNoLeak([]() {
        auto data_type = makeDataType("tuple(uint32, string)");

        cpython::PyObjectPtr py_list{PyList_New(1)};
        ASSERT_TRUE(py_list);

        cpython::PyObjectPtr row0{PyList_New(1)};
        ASSERT_TRUE(row0);
        PyList_SET_ITEM(row0.get(), 0, PyLong_FromUnsignedLong(1));
        PyList_SET_ITEM(py_list.get(), 0, row0.release());

        try
        {
            (void)cpython::convertPythonListToColumn(py_list, data_type);
            FAIL() << "Expected DB::Exception due to tuple/list arity mismatch";
        }
        catch (const DB::Exception &)
        {
        }
    });
}

TEST_F(CPythonTest, ColumnMap)
{
    assertNoLeak([]() {
        Map map1;
        map1.push_back(Tuple{String("hello"), String("world")});
        map1.push_back(Tuple{String("python"), String("udf")});

        Map map2;
        map2.push_back(Tuple{String("foo"), String("bar")});

        testColumnConvertion<ColumnMap, Map>(std::vector<Map>{map1, map2}, "map(string, string)", [](PyObject * item, const Map & v) {
            ASSERT_TRUE(PyDict_Check(item));
            ASSERT_EQ(PyDict_Size(item), v.size());

            PyObject * key = nullptr;
            PyObject * value = nullptr;
            Py_ssize_t pos = 0;

            for (size_t i = 0; i < v.size(); i++)
            {
                auto t = v[i].get<Tuple>();

                ASSERT_TRUE(PyDict_Next(item, &pos, &key, &value));

                ASSERT_TRUE(PyBytes_Check(key));
                ASSERT_EQ(pythonBytesAsString(key), t[0].get<String>());
                ASSERT_TRUE(PyBytes_Check(value));
                ASSERT_EQ(pythonBytesAsString(value), t[1].get<String>());
            }
        });
    });
}

TEST_F(CPythonTest, ColumnBool)
{
    assertNoLeak([]() {
        testColumnConvertion<ColumnUInt8, bool>(std::vector<bool>{true, false, true}, "bool", [](PyObject * item, const bool & v) {
            ASSERT_TRUE(PyBool_Check(item));
            ASSERT_EQ(Py_IsTrue(item), v);
        });
    });
}

TEST_F(CPythonTest, ColumnConstInt32)
{
    assertNoLeak([]() {
        auto data_type = makeDataType("int32");
        auto inner_col = data_type->createColumn();
        inner_col->insert(Field(Int32(42)));
        auto const_col = ColumnConst::create(std::move(inner_col), 3);

        auto py_list = cpython::convertColumnToPythonList(*const_col, data_type, 0, const_col->size());
        ASSERT_TRUE(py_list);
        ASSERT_TRUE(PyList_Check(py_list.get()));
        ASSERT_EQ(PyList_Size(py_list.get()), 3);

        for (Py_ssize_t i = 0; i < 3; i++)
        {
            PyObject * item = PyList_GetItem(py_list.get(), i);
            ASSERT_TRUE(PyLong_Check(item));
            ASSERT_EQ(PyLong_AsLong(item), 42);
        }
    });
}

TEST_F(CPythonTest, ColumnConstString)
{
    assertNoLeak([]() {
        auto data_type = makeDataType("string");
        auto inner_col = data_type->createColumn();
        inner_col->insert(Field(String("hello")));
        auto const_col = ColumnConst::create(std::move(inner_col), 2);

        auto py_list = cpython::convertColumnToPythonList(*const_col, data_type, 0, const_col->size());
        ASSERT_TRUE(py_list);
        ASSERT_TRUE(PyList_Check(py_list.get()));
        ASSERT_EQ(PyList_Size(py_list.get()), 2);

        for (Py_ssize_t i = 0; i < 2; i++)
        {
            PyObject * item = PyList_GetItem(py_list.get(), i);
            ASSERT_TRUE(PyBytes_Check(item));
            ASSERT_EQ(pythonBytesAsString(item), "hello");
        }
    });
}

TEST_F(CPythonTest, ColumnLowCardinalityString)
{
    assertNoLeak([]() {
        auto inner_type = makeDataType("string");
        auto lc_type = std::make_shared<DataTypeLowCardinality>(inner_type);
        auto lc_col = lc_type->createColumn();

        lc_col->insert(Field(String("alpha")));
        lc_col->insert(Field(String("beta")));
        lc_col->insert(Field(String("alpha")));

        auto py_list = cpython::convertColumnToPythonList(*lc_col, lc_type, 0, lc_col->size());
        ASSERT_TRUE(py_list);
        ASSERT_TRUE(PyList_Check(py_list.get()));
        ASSERT_EQ(PyList_Size(py_list.get()), 3);

        ASSERT_EQ(pythonBytesAsString(PyList_GetItem(py_list.get(), 0)), "alpha");
        ASSERT_EQ(pythonBytesAsString(PyList_GetItem(py_list.get(), 1)), "beta");
        ASSERT_EQ(pythonBytesAsString(PyList_GetItem(py_list.get(), 2)), "alpha");
    });
}

TEST_F(CPythonTest, ColumnLowCardinalityNullableInt32)
{
    assertNoLeak([]() {
        auto inner_type = makeDataType("nullable(int32)");
        auto lc_type = std::make_shared<DataTypeLowCardinality>(inner_type);
        auto lc_col = lc_type->createColumn();

        lc_col->insert(Field(Int32(10)));
        lc_col->insert(Field{});
        lc_col->insert(Field(Int32(10)));

        auto py_list = cpython::convertColumnToPythonList(*lc_col, lc_type, 0, lc_col->size());
        ASSERT_TRUE(py_list);
        ASSERT_TRUE(PyList_Check(py_list.get()));
        ASSERT_EQ(PyList_Size(py_list.get()), 3);

        ASSERT_TRUE(PyLong_Check(PyList_GetItem(py_list.get(), 0)));
        ASSERT_EQ(PyLong_AsLong(PyList_GetItem(py_list.get(), 0)), 10);

        ASSERT_TRUE(Py_IsNone(PyList_GetItem(py_list.get(), 1)));

        ASSERT_TRUE(PyLong_Check(PyList_GetItem(py_list.get(), 2)));
        ASSERT_EQ(PyLong_AsLong(PyList_GetItem(py_list.get(), 2)), 10);
    });
}

#if USE_NUMPY
bool init_numpy()
{
    import_array1(false) return true;
}


template <typename T>
std::pair<T *, npy_intp> getInternalDataAndSize(PyObject * np_array)
{
    PyArrayObject * numpy_array = reinterpret_cast<PyArrayObject *>(np_array);
    T * data = static_cast<T *>(PyArray_DATA(numpy_array));
    npy_intp size = PyArray_SIZE(numpy_array);
    return std::make_pair(data, size);
}


TEST_F(CPythonTest, columnAndNumpyConversion)
{
    setenv("PYTHONPATH", "/usr/lib/python3.10", 1);
    setenv("PYTHONHOME", "/usr/lib/python3.10", 1);
    ASSERT_TRUE(init_numpy());
#define GENERATE_TEST_1(TYPE, COLUMN_TYPE, DATA_TYPE) \
    { \
        auto col = COLUMN_TYPE::create(); \
        col->getData().push_back(1); \
        col->getData().push_back(2); \
        col->getData().push_back(3); \
        auto * address = col->getData().data(); \
        PyObject * np_array = cpython::convertColumnToNumpyArray(*col); \
        ASSERT_TRUE(np_array != nullptr); \
        auto [data, size] = getInternalDataAndSize<TYPE>(np_array); \
        ASSERT_EQ(size, 3); \
        ASSERT_EQ(data[0], 1); \
        ASSERT_EQ(data[1], 2); \
        ASSERT_EQ(data[2], 3); \
        /* check if the addres is the same(zero copy)*/ \
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
        PyObject * np_array = cpython::convertColumnToNumpyArray(*col);
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
        PyObject * np_array = cpython::convertColumnToNumpyArray(*col);
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
        PyObject * np_array = cpython::convertColumnToNumpyArray(*col);
        PyArrayObject * numpy_array = reinterpret_cast<PyArrayObject *>(np_array);
        PyObject ** result_data = static_cast<PyObject **>(PyArray_DATA(numpy_array));
        npy_intp size = PyArray_SIZE(numpy_array);
        ASSERT_EQ(size, column_with_type.column->size());
        for (npy_intp i = 0; i < size; i++)
        {
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
        PyObject * np_array = cpython::convertColumnToNumpyArray(*col);
        PyArrayObject * numpy_array = reinterpret_cast<PyArrayObject *>(np_array);
        int type = PyArray_TYPE(numpy_array);
        npy_intp size = PyArray_SIZE(numpy_array);
        ASSERT_EQ(size, column_with_type.column->size());

        // type of numpy array element is NPY_STRING
        ASSERT_EQ(type, NPY_STRING);
        char ** result_data = reinterpret_cast<char **>(PyArray_DATA(numpy_array));
        const auto & column_fixed = assert_cast<const ColumnFixedString &>(*(column_with_type.column));
        // check address
        auto result_data_ptr = reinterpret_cast<const char8_t *>(result_data);
        ASSERT_EQ(result_data_ptr, column_fixed.getChars().data());

        for (npy_intp i = 0; i < size; i++)
        {
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
        ColumnWithTypeAndName column_with_type(col, std::make_shared<DATA_TYPE>(), "column"); \
        PyObject * np_array = cpython::convertColumnToNumpyArray(*col); \
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
        GENERATE_TEST_2(ColumnUInt32, DataTypeUInt32) GENERATE_TEST_2(ColumnUInt64, DataTypeUInt64)
            GENERATE_TEST_2(ColumnInt8, DataTypeInt8) GENERATE_TEST_2(ColumnInt16, DataTypeInt16)
                GENERATE_TEST_2(ColumnInt32, DataTypeInt32) GENERATE_TEST_2(ColumnInt64, DataTypeInt64)
    }
}
#endif

#endif /// USE_PYTHON_UDF
