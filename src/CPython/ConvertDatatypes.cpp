#ifdef ENABLE_PYTHON_UDF
#    include <CPython/ConvertDatatypes.h>
#    include <CPython/validatePython.h>
#    include <Columns/ColumnArray.h>
#    include <Columns/ColumnFixedString.h>
#    include <Columns/ColumnMap.h>
#    include <Columns/ColumnString.h>
#    include <Columns/ColumnTuple.h>
#    include <Columns/ColumnsDateTime.h>
#    include <Columns/ColumnsNumber.h>
#    include <DataTypes/DataTypeArray.h>
#    include <DataTypes/DataTypeMap.h>
#    include <DataTypes/DataTypeTuple.h>
#    include <Common/Exception.h>

#    define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION
#    include <numpy/arrayobject.h>

#    include <functional>
namespace DB
{
namespace ErrorCodes
{
extern const int INTERNAL_ERROR;
}
namespace cpython
{

template <typename T, typename Convertor>
PyObject * columnToPythonList(const IColumn & col, UInt64 offset, UInt32 size, Convertor converter)
{
    const auto & internal_data = assert_cast<const T &>(col).getData();
    PyObject * py_list = PyList_New(size);
    for (size_t i = 0; i < size; i++)
    {
        PyList_SetItem(py_list, i, converter(internal_data[i + offset]));
    }
    return py_list;
}
PyObject * convertColumnToPythonList(const IColumn & column, const DataTypePtr & type, UInt64 offset, UInt64 size)
{
    auto arg_type_id = type->getTypeId();
    switch (arg_type_id)
    {
        case TypeIndex::UInt8:
            return columnToPythonList<ColumnUInt8>(column, offset, size, PyLong_FromUnsignedLong);
        case TypeIndex::UInt16:
            return columnToPythonList<ColumnUInt16>(column, offset, size, PyLong_FromUnsignedLong);
        case TypeIndex::UInt32:
            return columnToPythonList<ColumnUInt32>(column, offset, size, PyLong_FromUnsignedLong);
        case TypeIndex::UInt64:
            return columnToPythonList<ColumnUInt64>(column, offset, size, PyLong_FromUnsignedLongLong);
        case TypeIndex::Int8:
            return columnToPythonList<ColumnInt8>(column, offset, size, PyLong_FromLong);
        case TypeIndex::Int16:
            return columnToPythonList<ColumnInt16>(column, offset, size, PyLong_FromLong);
        case TypeIndex::Int32:
            return columnToPythonList<ColumnInt32>(column, offset, size, PyLong_FromLong);
        case TypeIndex::Int64:
            return columnToPythonList<ColumnInt64>(column, offset, size, PyLong_FromLongLong);
        case TypeIndex::Date:
            return columnToPythonList<ColumnDate>(column, offset, size, PyLong_FromUnsignedLong);
        case TypeIndex::Date32:
            return columnToPythonList<ColumnDate32>(column, offset, size, PyLong_FromLong);
        case TypeIndex::DateTime:
            return columnToPythonList<ColumnDateTime>(column, offset, size, PyLong_FromUnsignedLong);
        case TypeIndex::DateTime64: {
        }
        case TypeIndex::Float32:
            return columnToPythonList<ColumnFloat32>(column, offset, size, PyFloat_FromDouble);
        case TypeIndex::Float64:
            return columnToPythonList<ColumnFloat64>(column, offset, size, PyFloat_FromDouble);
        case TypeIndex::String: {
            const auto & column_str = assert_cast<const ColumnString &>(column);
            PyObject * py_list = PyList_New(size);
            for (size_t i = 0; i < size; i++)
            {
                PyObject * item = PyUnicode_FromString(column_str.getDataAt(offset + i).data);
                if (item == nullptr)
                {
                    Py_CLEAR(py_list);
                    throw Exception(ErrorCodes::INTERNAL_ERROR, "Failed to convert string to Python unicode");
                }
                PyList_SetItem(py_list, i, item);
            }
            return py_list;
        }
        case TypeIndex::FixedString: {
            const auto & column_fixed = assert_cast<const ColumnFixedString &>(column);
            PyObject * py_list = PyList_New(size);
            for (size_t i = 0; i < size; i++)
            {
                PyObject * item = PyUnicode_FromStringAndSize(column_fixed.getDataAt(offset + i).data, column_fixed.getN());
                if (item == nullptr)
                {
                    Py_CLEAR(py_list);
                    throw Exception(ErrorCodes::INTERNAL_ERROR, "Failed to convert fixed string to Python unicode");
                }
                PyList_SetItem(py_list, i, item);
            }
            return py_list;
        }
        case TypeIndex::Array: {
            const auto & array_type = assert_cast<const DataTypeArray &>(*type);
            const auto & column_array = assert_cast<const ColumnArray &>(column);
            PyObject * py_list = PyList_New(size);
            UInt64 tmp_offset = 0;
            for (size_t i = 0; i < size; i++)
            {
                UInt64 elem_offset = column_array.getOffsets()[i];
                UInt64 elem_size = elem_offset - tmp_offset;
                PyObject * elem
                    = convertColumnToPythonList(column_array.getData(), array_type.getNestedType(), offset + tmp_offset, elem_size);
                tmp_offset = elem_offset;
                // for (size_t j = 0; j < PyList_Size(elem); j++)
                // {
                //     auto item = PyList_GetItem(elem, j);
                //     [[maybe_unused]] auto value = PyLong_AsUnsignedLong(item);
                //     [[maybe_unused]] int b = 0;
                // }
                PyList_SetItem(py_list, i, elem);
            }
            return py_list;
        }
        case TypeIndex::Tuple: {
            const auto & tuple_type = assert_cast<const DataTypeTuple &>(*type);
            const auto & column_tuple = assert_cast<const ColumnTuple &>(column);

            size_t tuple_size = tuple_type.getElements().size();
            PyObject * py_tuple_list = PyList_New(tuple_size);
            for (size_t i = 0; i < tuple_size; i++)
            {
                const auto & internal_column = column_tuple.getColumn(i);
                PyObject * item = convertColumnToPythonList(internal_column, tuple_type.getElement(i), offset, internal_column.size());
                PyList_SetItem(py_tuple_list, i, item);
            }

            PyObject * py_list = PyList_New(size);
            for (size_t i = 0; i < size; i++)
            {
                PyObject * tuple = PyTuple_New(tuple_size);
                for (size_t j = 0; j < tuple_size; j++)
                {
                    PyObject * item = PyList_GetItem(py_tuple_list, j);
                    PyTuple_SetItem(tuple, j, PyList_GetItem(item, i));
                }
                PyList_SetItem(py_list, i, tuple);
            }
            // Py_CLEAR(py_tuple_list);

            return py_list;
        }
        // case TypeIndex::Map:
        // {
        //     // no need consider array(map(key, value)), because it does not make sense at all.
        //     const auto & map_type = assert_cast<const DataTypeMap &>(*type);

        //     // column map stores data as ColumnArray(ColumnTuple(key, value))
        //     const auto & column_map = assert_cast<const ColumnMap &>(column);
        //     const auto & column_tuple = assert_cast<const ColumnTuple &>(column_map.getNestedColumn().getData());
        //     const auto & column_keys = column_tuple.getColumn(0);
        //     const auto & column_values = column_tuple.getColumn(1);
        //     PyObject * key_list = convertColumnToPythonList(column_keys, map_type.getKeyType(), offset, size);
        //     PyObject * value_list = convertColumnToPythonList(column_values, map_type.getValueType(), offset, size);
        //     PyObject * dict = PyDict_New();
        //     for (size_t i = 0; i < size; i++)
        //     {
        //         PyObject * key = PyList_GetItem(key_list, i);
        //         PyObject * value = PyList_GetItem(value_list, i);
        //         Py_INCREF(key);
        //         Py_INCREF(value);
        //         PyDict_SetItem(dict, key, value);
        //     }
        //     Py_DECREF(key_list);
        //     Py_DECREF(value_list);
        //     return dict;
        // }
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Python UDF does not support data type: {}", column.getName());
    }

    return nullptr;
}

PyObject * convertColumnToPythonList(const ColumnWithTypeAndName & column_with_type)
{
    const auto & column = column_with_type.column;
    return convertColumnToPythonList(*column, column_with_type.type, 0, column->size());
}

template <typename T, typename element_type, typename Convertor>
void PythonListToColumn(PyObject * py_list, IColumn & column, Convertor converter)
{
    size_t size = PyList_Size(py_list);
    auto & data = assert_cast<T &>(column).getData();
    // data.reserve(size);
    for (size_t i = 0; i < size; i++)
    {
        PyObject * item = PyList_GetItem(py_list, i);
        /// need check overflow
        data.push_back(static_cast<element_type>(converter(item)));
    }
}

void convertPythonListToColumn(PyObject * py_list, IColumn & column, const DataTypePtr & type, bool need_release)
{
    auto arg_type_id = type->getTypeId();
    switch (arg_type_id)
    {
        case TypeIndex::UInt8: {
            PythonListToColumn<ColumnUInt8, UInt8>(py_list, column, PyLong_AsUnsignedLong);
            break;
        }
        case TypeIndex::UInt16: {
            PythonListToColumn<ColumnUInt16, UInt16>(py_list, column, PyLong_AsUnsignedLong);
            break;
        }
        case TypeIndex::UInt32: {
            PythonListToColumn<ColumnUInt32, UInt32>(py_list, column, PyLong_AsUnsignedLong);
            break;
        }
        case TypeIndex::UInt64: {
            PythonListToColumn<ColumnUInt64, UInt64>(py_list, column, PyLong_AsUnsignedLongLong);
            break;
        }
        case TypeIndex::Int8: {
            PythonListToColumn<ColumnInt8, Int8>(py_list, column, PyLong_AsLong);
            break;
        }
        case TypeIndex::Int16: {
            PythonListToColumn<ColumnInt16, Int16>(py_list, column, PyLong_AsLong);
            break;
        }
        case TypeIndex::Int32: {
            PythonListToColumn<ColumnInt32, Int32>(py_list, column, PyLong_AsLong);
            break;
        }
        case TypeIndex::Int64: {
            PythonListToColumn<ColumnInt64, Int64>(py_list, column, PyLong_AsLongLong);
            break;
        }
        case TypeIndex::Date: {
            PythonListToColumn<ColumnDate, UInt16>(py_list, column, PyLong_AsUnsignedLong);
            break;
        }
        case TypeIndex::Date32: {
            PythonListToColumn<ColumnDate32, UInt32>(py_list, column, PyLong_AsUnsignedLong);
            break;
        }
        case TypeIndex::DateTime: {
            PythonListToColumn<ColumnDateTime, UInt32>(py_list, column, PyLong_AsUnsignedLong);
            break;
        }
        // case TypeIndex::DateTime64:
        // {
        //     PythonListToColumn<ColumnDateTime64, Int64>(py_list, column, PyLong_AsUnsignedLongLong);
        //     break;
        // }
        case TypeIndex::Float32: {
            PythonListToColumn<ColumnFloat32, Float32>(py_list, column, PyFloat_AsDouble);
            break;
        }
        case TypeIndex::Float64: {
            PythonListToColumn<ColumnFloat64, Float64>(py_list, column, PyFloat_AsDouble);
            break;
        }
        case TypeIndex::String: {
            auto & column_str = assert_cast<ColumnString &>(column);
            size_t size = PyList_Size(py_list);
            for (size_t i = 0; i < size; i++)
            {
                PyObject * item = PyList_GetItem(py_list, i);
                const char * str = PyUnicode_AsUTF8(item);
                column_str.insertData(str, strlen(str));
            }
            break;
        }
        case TypeIndex::FixedString: {
            auto & column_fixed = assert_cast<ColumnFixedString &>(column);
            size_t size = PyList_Size(py_list);
            for (size_t i = 0; i < size; i++)
            {
                PyObject * item = PyList_GetItem(py_list, i);
                const char * str = PyUnicode_AsUTF8(item);
                column_fixed.insertData(str, strlen(str));
            }
            break;
        }
        case TypeIndex::Array: {
            const auto & array_type = assert_cast<const DataTypeArray &>(*type);
            auto & column_array = assert_cast<ColumnArray &>(column);
            size_t size = PyList_Size(py_list);

            /// get the nested column
            auto & nested_column = column_array.getData();
            for (size_t i = 0; i < size; i++)
            {
                PyObject * elem = PyList_GetItem(py_list, i);
                UInt64 elem_size = PyList_Size(elem);
                /// no need release the list element. because we will release the list in the end, then the element will be released too.
                convertPythonListToColumn(elem, nested_column, array_type.getNestedType(), false);
                column_array.getOffsets().push_back(column_array.getOffsets().back() + elem_size);
            }
            break;
        }
        case TypeIndex::Tuple: {
            const auto & tuple_type = assert_cast<const DataTypeTuple &>(*type);
            auto & column_tuple = assert_cast<ColumnTuple &>(column);
            size_t tuple_size = tuple_type.getElements().size();
            size_t size = PyList_Size(py_list);
            /// [(1, 'a'), (2, 'b')] -> [[1, 2], ['a', 'b']]
            PyObject * tuple_list_unzip = PyList_New(tuple_size);
            for (size_t i = 0; i < tuple_size; i++)
            {
                PyObject * item = PyList_New(size);
                for (size_t j = 0; j < size; j++)
                {
                    PyObject * tuple = PyList_GetItem(py_list, j);
                    PyObject * elem = PyTuple_GetItem(tuple, i);
                    Py_INCREF(elem); // 增加引用计数
                    PyList_SetItem(item, j, elem);
                }
                PyList_SetItem(tuple_list_unzip, i, item);
            }

            for (size_t i = 0; i < tuple_size; i++)
            {
                PyObject * item = PyList_GetItem(tuple_list_unzip, i);
                convertPythonListToColumn(item, column_tuple.getColumn(i), tuple_type.getElement(i), false);
            }

            /// release the tuple_list_unzip
            Py_DECREF(tuple_list_unzip);
            break;
        }
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Python UDF does not support data type: {}", column.getName());
    }
    if (need_release)
        Py_CLEAR(py_list);
}

ColumnPtr convertPythonListToColumn(PyObject * py_list, const DataTypePtr & type)
{
    auto column = type->createColumn();
    convertPythonListToColumn(py_list, *column, type);
    return column;
}

template <typename T>
PyObject * columnToNumpyArray(const IColumn & col, NPY_TYPES npy_type)
{
    const auto & internal_data = assert_cast<const T &>(col).getData();
    npy_intp size = col.size();
    npy_intp dims[1] = {size};
    PyObject * py_array = PyArray_SimpleNewFromData(1, dims, npy_type, (void *)internal_data.data());
    return py_array;
}
PyObject * convertColumnToNumpyArray(const IColumn & column)
{
    import_array() auto arg_type_id = column.getDataType();
    switch (arg_type_id)
    {
        case TypeIndex::UInt8:
            return columnToNumpyArray<ColumnUInt8>(column, NPY_UBYTE);
        case TypeIndex::UInt16:
            return columnToNumpyArray<ColumnUInt16>(column, NPY_USHORT);
        case TypeIndex::UInt32:
            return columnToNumpyArray<ColumnUInt32>(column, NPY_UINT);
        case TypeIndex::UInt64:
            return columnToNumpyArray<ColumnUInt64>(column, NPY_ULONG);
        case TypeIndex::Int8:
            return columnToNumpyArray<ColumnInt8>(column, NPY_BYTE);
        case TypeIndex::Int16:
            return columnToNumpyArray<ColumnInt16>(column, NPY_SHORT);
        case TypeIndex::Int32:
            return columnToNumpyArray<ColumnInt32>(column, NPY_INT);
        case TypeIndex::Int64:
            return columnToNumpyArray<ColumnInt64>(column, NPY_LONG);
        case TypeIndex::Float32:
            return columnToNumpyArray<ColumnFloat32>(column, NPY_FLOAT);
        case TypeIndex::Float64:
            return columnToNumpyArray<ColumnFloat64>(column, NPY_DOUBLE);
        case TypeIndex::IPv4:
            return columnToNumpyArray<ColumnIPv4>(column, NPY_UINT);
        case TypeIndex::Date:
            return columnToNumpyArray<ColumnDate>(column, NPY_USHORT);
        case TypeIndex::Date32:
            return columnToNumpyArray<ColumnDate32>(column, NPY_INT);
        case TypeIndex::DateTime:
            return columnToNumpyArray<ColumnDateTime>(column, NPY_UINT);
        case TypeIndex::DateTime64:
            return columnToNumpyArray<ColumnDateTime64>(column, NPY_UINT);
        case TypeIndex::String: {
            const auto & column_str = assert_cast<const ColumnString &>(column);
            npy_intp size = column_str.size();
            npy_intp dims[1] = {size};
            PyObject * np_array = PyArray_SimpleNew(1, dims, NPY_OBJECT);
            for (npy_intp i = 0; i < size; i++)
            {
                PyObject * item = PyUnicode_FromString(column_str.getDataAt(i).data);
                if (item == nullptr)
                {
                    Py_CLEAR(np_array);
                    throw std::runtime_error("Failed to convert string to Python unicode");
                }
                char * itemptr = reinterpret_cast<char *>(PyArray_GetPtr((PyArrayObject *)np_array, &i));
                PyArray_SETITEM((PyArrayObject *)np_array, itemptr, item);
            }
            return np_array;
        }
        case TypeIndex::FixedString: {
            const auto & column_fixed = assert_cast<const ColumnFixedString &>(column);
            npy_intp size = column_fixed.size();
            npy_intp dims[1] = {size};

            size_t fixed_size = column_fixed.getN();
            // S{fixed_size} means string of length fixed_size
            std::string fixed_type = "S" + std::to_string(fixed_size);
            PyObject * typestr = PyUnicode_FromString(fixed_type.c_str());
            PyArray_Descr * descr;
            if (PyArray_DescrConverter(typestr, &descr) != NPY_SUCCEED)
            {
                std::string error_message = catchException();
                throw Exception(ErrorCodes::INTERNAL_ERROR, "Create fixed string pattern failed, detail message : {}", error_message);
            }
            PyObject * np_array
                = PyArray_NewFromDescr(&PyArray_Type, descr, 1, dims, NULL, (void *)column_fixed.getChars().data(), NPY_ARRAY_CARRAY, NULL);

            return np_array;
        }
        case TypeIndex::Array: {
        }

        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Python UDF does not support data type: {}", column.getDataType());
    }
    return nullptr;
}
/**
 * @brief convert column to numpy array, not all the data type can be zero copy.
 * below is the data types that can be zero copy:
 * UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64, IPv4, Date, Date32, DateTime, DateTime64
 * FixedString
 * 
 * @param column_with_type 
 * @return PyObject* 
 */
PyObject * convertColumnToNumpyArray(const ColumnWithTypeAndName & column_with_type)
{
    const auto & column = column_with_type.column;
    return convertColumnToNumpyArray(*column);
}

template <typename column_type, typename element_type>
void numpyToColumn(PyObject * py_array, IColumn & column)
{
    auto & internal_data = assert_cast<column_type &>(column).getData();
    auto np_array = reinterpret_cast<PyArrayObject *>(py_array);
    npy_intp size = PyArray_SIZE(np_array);
    auto data__raw_ptr = static_cast<element_type *>(PyArray_DATA(np_array));
    internal_data.insert(data__raw_ptr, data__raw_ptr + size);
}

void covertNumpyArrayToColumn(PyObject * py_array, IColumn & column)
{
    auto type = column.getDataType();
    switch (type)
    {
        case TypeIndex::UInt8: {
            numpyToColumn<ColumnUInt8, UInt8>(py_array, column);
            break;
        }
        case TypeIndex::UInt16: {
            numpyToColumn<ColumnUInt16, UInt16>(py_array, column);
            break;
        }
        case TypeIndex::UInt32: {
            numpyToColumn<ColumnUInt32, UInt32>(py_array, column);
            break;
        }
        case TypeIndex::UInt64: {
            numpyToColumn<ColumnUInt64, UInt64>(py_array, column);
            break;
        }
        case TypeIndex::Int8: {
            numpyToColumn<ColumnInt8, Int8>(py_array, column);
            break;
        }
        case TypeIndex::Int16: {
            numpyToColumn<ColumnInt16, Int16>(py_array, column);
            break;
        }
        case TypeIndex::Int32: {
            numpyToColumn<ColumnInt32, Int32>(py_array, column);
            break;
        }
        case TypeIndex::Int64: {
            numpyToColumn<ColumnInt64, Int64>(py_array, column);
            break;
        }
        case TypeIndex::Float32: {
            numpyToColumn<ColumnFloat32, Float32>(py_array, column);
            break;
        }
        case TypeIndex::Float64: {
            numpyToColumn<ColumnFloat64, Float64>(py_array, column);
            break;
        }
        case TypeIndex::IPv4: {
            numpyToColumn<ColumnIPv4, IPv4>(py_array, column);
            break;
        }
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Python UDF does not support data type: {}", column.getFamilyName());
    }
}
// now we will copy all the result from numpy to column, it need to be optimized, if it can be zero copy.
ColumnPtr covertNumpyArrayToColumn(PyObject * py_array, const DataTypePtr & type)
{
    auto column = type->createColumn();
    covertNumpyArrayToColumn(py_array, *column);
    return column;
}


}
}
#endif