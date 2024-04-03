#include <CPython/ConvertDatatypes.h>
#include <Columns/ColumnsNumber.h>

namespace DB
{

namespace cpython
{


#define FOR_PYTHON_BASIC_NUMERIC_TYPES(M) \
    M(UInt16) \
    M(UInt32) \
    M(UInt64) \
    M(Int8) \
    M(Int16) \
    M(Int32) \
    M(Int64) \
    M(Float32) \
    M(Float64)

PyObject * convertColumnToPythonList(const ColumnWithTypeAndName & column_with_type)
{
    auto arg_type_id = column_with_type.type->getTypeId();

    const auto & column = column_with_type.column;
    switch(arg_type_id)
    {
        case TypeIndex::UInt8:
        {
            break;
        }
        case TypeIndex::UInt16:
        {
            PyObject * py_list = PyList_New(column->size());
            const auto & internal_data = assert_cast<const ColumnUInt16 &>(*column).getData();
            for(size_t i = 0; i < column->size(); i++)
            {
                PyList_SetItem(py_list, i, PyLong_FromLong(internal_data[i]));
            }
            return py_list;
        }
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Python UDF does not support data type: {}", column_with_type.type->getName());
    }

    return nullptr;
}

PyObject * convertColumnToPythonList(const ColumnPtr & column)
{
    auto arg_type_id = column->getDataType();
    switch(arg_type_id)
    {
        case TypeIndex::UInt16:
        {
            PyObject * py_list = PyList_New(column->size());
            const auto & internal_data = assert_cast<const ColumnUInt16 &>(*column).getData();
            for(size_t i = 0; i < column->size(); i++)
            {
                PyList_SetItem(py_list, i, PyLong_FromLong(internal_data[i]));
            }
            return py_list;
        }
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Python UDF does not support data type: {}", column->getName());
    }

    return nullptr;
}


ColumnPtr convertPythonListToColumn(PyObject * py_list, const DataTypePtr & arg_type)
{
    auto arg_type_id = arg_type->getTypeId();
    switch(arg_type_id)
    {
        case TypeIndex::UInt16:
        {
            auto column = ColumnVector<UInt16>::create();
            // column->getData().resize(PyList_Size(py_list));
            for(size_t i = 0; i < PyList_Size(py_list); i++)
            {
                PyObject * item = PyList_GetItem(py_list, i);
                column->insert(PyLong_AsLong(item));
            }
            return column;
        }
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Python UDF does not support data type: {}", arg_type->getName());
    }

}


// PyArrayObject * convertColumnToNumpyArray(const ColumnWithTypeAndName & column_with_type)
// {
//     auto arg_type_id = column_with_type.type->getTypeId();

//     const auto & column = column_with_type.column;
//     switch (arg_type_id)
//     {
//     case TypeIndex::UInt8:
//     {
//         const auto & internal_data = assert_cast<const ColumnUInt8 &>(*column).getData();
//         npy_intp size = column->size();
//         npy_intp dims[1] = {size};
//         PyObject * py_array = PyArray_SimpleNewFromData(1, dims, NPY_USHORT, (void *)(internal_data.data()));
//         return py_array;
//     }
//     case TypeIndex::Int32:
//     {
//         const auto & internal_data = assert_cast<const ColumnInt32 &>(*column).getData();
//         npy_intp size = column->size();
//         npy_intp dims[1] = {size};
//         PyObject * py_array = PyArray_SimpleNewFromData(1, dims, NPY_INT32, (void *)(internal_data.data()));
//         return reinterpret_cast<PyArrayObject*>(py_array);
//     }
//     default:
//         throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Python UDF does not support data type: {}", column_with_type.type->getName());
//     }
//     return nullptr;
// }

}
}