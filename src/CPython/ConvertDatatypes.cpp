#include <CPython/ConvertDatatypes.h>
#include <CPython/Utils.h>
#include <CPython/validatePython.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <Core/DecimalFunctions.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>
#include <Common/DateLUT.h>
#include <Common/Exception.h>
#include <Common/LocalDate.h>
#include <Common/LocalDateTime.h>
#include <Common/LocalDateTime64.h>

#include <datetime.h>

#if USE_NUMPY
#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION
#include <numpy/arrayobject.h>
#endif

namespace DB
{
namespace ErrorCodes
{
extern const int UDF_INTERNAL_ERROR;
}
namespace cpython
{

namespace
{
void raiseConvertionException(const std::string & type_name, PyObject * object, const std::string & reason = std::string())
{
    throw Exception(
        ErrorCodes::UDF_INTERNAL_ERROR,
        "Failed to convert Python object to {} {}{}",
        type_name,
        convertPyObjectToString(PyObjectPtr::borrow(object)),
        reason.empty() ? "" : fmt::format(": {}", reason));
}

template <typename T, typename Convertor>
PyObjectPtr convertToPythonObject(const Field & data, Convertor convertor)
{
    using U = NearestFieldType<std::decay_t<T>>;
    static constexpr Field::Types::Which TYPE_ENUM = Field::TypeToEnum<U>::value;

    static_assert(TYPE_ENUM != Field::Types::Which::Array);
    static_assert(TYPE_ENUM != Field::Types::Which::Tuple);
    static_assert(TYPE_ENUM != Field::Types::Which::Map);
    static_assert(TYPE_ENUM != Field::Types::Which::Object);

    U value = data.get<U>();

    PyObjectPtr item;

    if constexpr (TYPE_ENUM == Field::Types::String)
        item.reset(convertor(value.data(), value.size()));
    else
        item.reset(convertor(value));

    if (item)
        return item;

    throw Exception(ErrorCodes::UDF_INTERNAL_ERROR, "Failed to convert {} to Python object", fieldTypeToString(data.getType()));
}

PyObject * convertUInt16ToDate(UInt16 date_value)
{
    PyDateTime_IMPORT;
    LocalDate local_date{DayNum(date_value)};

    return PyDate_FromDate(local_date.year(), local_date.month(), local_date.day());
}

PyObject * convertInt32ToDate(Int32 date_value)
{
    PyDateTime_IMPORT;

    LocalDate local_date{ExtendedDayNum(date_value)};

    return PyDate_FromDate(local_date.year(), local_date.month(), local_date.day());
}

PyObjectPtr convertUInt32ToDateTime(const Field & data, const String & time_zone)
{
    PyDateTime_IMPORT;

    UInt32 date_value = data.get<UInt32>();

    const auto & date_lut = DateLUT::instance(time_zone);
    LocalDateTime local_datetime{date_value, date_lut};

    PyObjectPtr offset{PyDelta_FromDSU(0, date_lut.getOffsetAtStartOfEpoch(), 0)};
    PyObjectPtr name{PyUnicode_FromString(time_zone.data())};
    PyObjectPtr tzinfo{PyDateTimeAPI->TimeZone_FromTimeZone(offset.get(), name.get())};

    return PyObjectPtr{PyDateTimeAPI->DateTime_FromDateAndTime(
        local_datetime.year(),
        local_datetime.month(),
        local_datetime.day(),
        local_datetime.hour(),
        local_datetime.minute(),
        local_datetime.second(),
        0,
        tzinfo.get(),
        PyDateTimeAPI->DateTimeType)};
}

PyObjectPtr convertDateTime64ToPythonObject(const Field & data, UInt32 scale, const String & time_zone)
{
    PyDateTime_IMPORT;

    DateTime64 datetime64 = data.get<DateTime64>();

    const auto & date_lut = DateLUT::instance(time_zone);
    LocalDateTime64 local_datetime{datetime64, scale, date_lut};

    PyObjectPtr offset{PyDelta_FromDSU(0, date_lut.getOffsetAtStartOfEpoch(), 0)};
    PyObjectPtr name{PyUnicode_FromString(time_zone.data())};
    PyObjectPtr tzinfo{PyDateTimeAPI->TimeZone_FromTimeZone(offset.get(), name.get())};

    return PyObjectPtr{PyDateTimeAPI->DateTime_FromDateAndTime(
        local_datetime.year(),
        local_datetime.month(),
        local_datetime.day(),
        local_datetime.hour(),
        local_datetime.minute(),
        local_datetime.second(),
        local_datetime.microsecond(),
        tzinfo.get(),
        PyDateTimeAPI->DateTimeType)};
}

std::optional<int64_t> getOffsetFromDatetime(PyObject * obj)
{
    PyObjectPtr offset = PyObjectPtr{PyObject_CallMethod(obj, "utcoffset", "", Py_None)};
    if (!offset || Py_IsNone(offset.get()))
        return std::nullopt;

    if (!PyDelta_Check(offset.get()))
        raiseConvertionException("datetime", obj, "");

    auto total_seconds = PyObjectPtr{PyObject_CallMethod(offset.get(), "total_seconds", "", Py_None)};
    if (!total_seconds || Py_IsNone(total_seconds.get()) || !PyFloat_Check(total_seconds.get()))
        raiseConvertionException("timedelta", offset.get(), "failed to get the offset of object's timezone");

    auto seconds = PyFloat_AsDouble(total_seconds.get());
    if (PyErr_Occurred())
        raiseConvertionException("timedelta", offset.get(), "failed to get the offset of object's timezone");

    return {static_cast<int64_t>(seconds)};
};

} // namespace

PyObjectPtr convertToPythonObject(const Field & data, const DataTypePtr & type)
{
    auto type_id = type->getTypeId();
    switch (type_id)
    {
        // case TypeIndex::Nothing = 0:
        //     break;
        case TypeIndex::UInt8:
        {
            if (type->getName() == "bool")
                return convertToPythonObject<NearestFieldType<bool>>(data, PyBool_FromLong);
            else
                return convertToPythonObject<UInt8>(data, PyLong_FromUnsignedLong);
        }
        case TypeIndex::UInt16:
            return convertToPythonObject<UInt16>(data, PyLong_FromUnsignedLong);
        case TypeIndex::UInt32:
            return convertToPythonObject<UInt32>(data, PyLong_FromUnsignedLong);
        case TypeIndex::UInt64:
            return convertToPythonObject<UInt64>(data, PyLong_FromUnsignedLongLong);
        // case TypeIndex::UInt128:
        //     break;
        // case TypeIndex::UInt256:
        //     break;
        case TypeIndex::Int8:
            return convertToPythonObject<Int8>(data, PyLong_FromLong);
        case TypeIndex::Int16:
            return convertToPythonObject<Int16>(data, PyLong_FromLong);
        case TypeIndex::Int32:
            return convertToPythonObject<Int32>(data, PyLong_FromLong);
        case TypeIndex::Int64:
            return convertToPythonObject<Int64>(data, PyLong_FromLongLong);
        // case TypeIndex::Int128:
        //     break;
        // case TypeIndex::Int256:
        //     break;
        case TypeIndex::Float32:
            return convertToPythonObject<Float32>(data, PyFloat_FromDouble);
        case TypeIndex::Float64:
            return convertToPythonObject<Float32>(data, PyFloat_FromDouble);
        case TypeIndex::Date:
            return convertToPythonObject<UInt16>(data, convertUInt16ToDate);
        case TypeIndex::Date32:
            return convertToPythonObject<Int32>(data, convertInt32ToDate);
        case TypeIndex::DateTime:
            return convertUInt32ToDateTime(data, getDateTimeTimezone(*type));
        case TypeIndex::DateTime64:
        {
            auto datetime64_type = std::dynamic_pointer_cast<const DataTypeDateTime64>(type);
            return convertDateTime64ToPythonObject(data, datetime64_type->getScale(), getDateTimeTimezone(*type));
        }
        case TypeIndex::String:
            return convertToPythonObject<String>(data, PyUnicode_FromStringAndSize);
        case TypeIndex::FixedString:
            return convertToPythonObject<String>(data, PyUnicode_FromStringAndSize);
        // case TypeIndex::Enum8:
        //     break;
        // case TypeIndex::Enum16:
        //     break;
        // case TypeIndex::Decimal32:
        //     break;
        // case TypeIndex::Decimal64:
        //     break;
        // case TypeIndex::Decimal128:
        //     break;
        // case TypeIndex::Decimal256:
        //     break;
        // case TypeIndex::UUID:
        //     break;
        case TypeIndex::Array:
        {
            auto array_type = std::dynamic_pointer_cast<const DataTypeArray>(type);
            auto nested_type = array_type->getNestedType();

            Array value = data.get<Array>();
            PyObjectPtr py_list{PyList_New(value.size())};
            for (size_t i = 0; i < value.size(); i++)
            {
                auto item = convertToPythonObject(value[i], nested_type);
                PyList_SetItem(py_list.get(), i, item.release());
            }

            return py_list;
        }
        case TypeIndex::Tuple:
        {
            auto tuple_type = std::dynamic_pointer_cast<const DataTypeTuple>(type);

            Tuple value = data.get<Tuple>();

            PyObjectPtr py_list{PyTuple_New(value.size())};
            for (size_t i = 0; i < value.size(); i++)
            {
                auto item = convertToPythonObject(value[i], tuple_type->getElement(i));
                PyTuple_SetItem(py_list.get(), i, item.release());
            }

            return py_list;
        }
        // case TypeIndex::Set:
        //     break;
        // case TypeIndex::Interval:
        //     break;
        // case TypeIndex::Nullable:
        //     break;
        // case TypeIndex::Function:
        //     break;
        // case TypeIndex::AggregateFunction:
        //     break;
        // case TypeIndex::LowCardinality:
        //     break;
        case TypeIndex::Map:
        {
            Map value;
            if (data.tryGet<Map>(value))
            {
                PyObjectPtr py_dict{PyDict_New()};
                auto map_type = std::dynamic_pointer_cast<const DataTypeMap>(type);
                DataTypePtr pair_type = std::make_shared<const DataTypeTuple>(map_type->getKeyValueTypes());

                for (size_t i = 0; i < value.size(); i++)
                {
                    auto tuple = convertToPythonObject(value[i], pair_type);
                    if (!PyTuple_Check(tuple.get()) && PyTuple_Size(tuple.get()) != 2)
                        throw Exception(
                            ErrorCodes::UDF_INTERNAL_ERROR, "Failed to convert {} to Python tuple", fieldTypeToString(data.getType()));
                    auto key = PyObjectPtr::borrow(PyTuple_GetItem(tuple.get(), 0));
                    auto value = PyObjectPtr::borrow(PyTuple_GetItem(tuple.get(), 1));
                    PyDict_SetItem(py_dict.get(), key.release(), value.release());
                }

                return py_dict;
            }
        }
        // case TypeIndex::Object:
        //     break;
        /// DataType bool is DataTypeNumber<UInt8> with name "bool",
        /// Its typeid is still TypeIndex::UInt8. This branch will not be executed now.
        case TypeIndex::Bool:
            return convertToPythonObject<NearestFieldType<bool>>(data, PyBool_FromLong);
        case TypeIndex::IPv4:
            return convertToPythonObject<IPv4>(data, PyLong_FromUnsignedLong);
        // case TypeIndex::IPv6:
        //     break;
        default:
            throw Exception(ErrorCodes::UDF_INTERNAL_ERROR, "Failed to convert {} to Python object", fieldTypeToString(data.getType()));
    }
}

PyObjectPtr columnToPythonList(const IColumn & col, const DataTypePtr & type, UInt64 offset, UInt32 size)
{
    PyObjectPtr py_list{PyList_New(size)};
    for (size_t i = 0; i < size; i++)
    {
        auto item = convertToPythonObject(col[i + offset], type);
        PyList_SetItem(py_list.get(), i, item.release());
    }

    return py_list;
}

ALWAYS_INLINE bool loadFromPyBool(PyObject * object)
{
    if (!PyBool_Check(object))
        raiseConvertionException("bool", object);

    return static_cast<bool>(Py_IsTrue(object));
}

template <typename T>
ALWAYS_INLINE std::decay_t<T> loadFromPyUnsignedLong(PyObject * object)
{
    if (!PyLong_Check(object))
        raiseConvertionException("unsigned long", object);

    return static_cast<std::decay_t<T>>(PyLong_AsUnsignedLong(object));
}

template <>
ALWAYS_INLINE UInt64 loadFromPyUnsignedLong<UInt64>(PyObject * object)
{
    if (!PyLong_Check(object))
        raiseConvertionException("unsigned long", object);

    return PyLong_AsUnsignedLongLong(object);
}

template <typename T>
ALWAYS_INLINE std::decay_t<T> loadFromPyLong(PyObject * object)
{
    if (!PyLong_Check(object))
        raiseConvertionException("long", object);

    return static_cast<std::decay_t<T>>(PyLong_AsLong(object));
}

template <>
ALWAYS_INLINE Int64 loadFromPyLong<Int64>(PyObject * object)
{
    if (!PyLong_Check(object))
        raiseConvertionException("long", object);

    return PyLong_AsLongLong(object);
}

template <typename T>
ALWAYS_INLINE std::decay_t<T> loadFromPyFloat(PyObject * object)
{
    if (!PyFloat_Check(object))
        raiseConvertionException("float", object);

    return static_cast<std::decay_t<T>>(PyFloat_AsDouble(object));
}

ALWAYS_INLINE String loadFromPyUnicode(PyObject * object)
{
    if (!PyUnicode_Check(object))
        raiseConvertionException("string", object);

    return String{PyUnicode_AsUTF8(object)};
}

ALWAYS_INLINE UInt16 loadDate16FromPyDate(PyObject * object)
{
    PyDateTime_IMPORT;

    if (!PyDate_Check(object))
        raiseConvertionException("date16", object);

    auto year = static_cast<unsigned short>(PyDateTime_GET_YEAR(object));
    auto month = static_cast<unsigned char>(PyDateTime_GET_MONTH(object));
    auto day = static_cast<unsigned char>(PyDateTime_GET_DAY(object));

    LocalDate local_date{year, month, day};

    return local_date.getDayNum().toUnderType();
}

ALWAYS_INLINE Int32 loadDate32FromPyDate(PyObject * object)
{
    PyDateTime_IMPORT;

    if (!PyDate_Check(object))
        raiseConvertionException("date32", object);

    auto year = static_cast<unsigned short>(PyDateTime_GET_YEAR(object));
    auto month = static_cast<unsigned char>(PyDateTime_GET_MONTH(object));
    auto day = static_cast<unsigned char>(PyDateTime_GET_DAY(object));

    LocalDate local_date{year, month, day};
    return local_date.getExtenedDayNum().toUnderType();
}

ALWAYS_INLINE UInt32 loadDateTimeFromPyDateTime(PyObject * object)
{
    PyDateTime_IMPORT;

    if (!PyDateTime_Check(object))
        raiseConvertionException("datetime", object);

    try
    {
        auto offset = getOffsetFromDatetime(object);

        const auto & time_zone = offset.has_value() ? DateLUT::instance(offset.value()) : DateLUT::instance();

        auto year = static_cast<unsigned short>(PyDateTime_GET_YEAR(object));
        auto month = static_cast<unsigned char>(PyDateTime_GET_MONTH(object));
        auto day = static_cast<unsigned char>(PyDateTime_GET_DAY(object));
        auto hour = static_cast<unsigned char>(PyDateTime_DATE_GET_HOUR(object));
        auto minute = static_cast<unsigned char>(PyDateTime_DATE_GET_MINUTE(object));
        auto second = static_cast<unsigned char>(PyDateTime_DATE_GET_SECOND(object));

        LocalDateTime local_datetime{year, month, day, hour, minute, second};

        return static_cast<UInt32>(local_datetime.to_time_t(time_zone));
    }
    catch(const Exception & e)
    {
        raiseConvertionException("datetime", object, e.displayText());
    }

    UNREACHABLE();
}

ALWAYS_INLINE DateTime64 loadDateTime64FromPyDateTime(PyObject * object, UInt32 scale)
{
    PyDateTime_IMPORT;

    if (!PyDateTime_Check(object))
        raiseConvertionException("datetime64", object);

    try
    {
        auto offset = getOffsetFromDatetime(object);

        const auto & time_zone = offset.has_value() ? DateLUT::instance(offset.value()) : DateLUT::instance();


        auto year = static_cast<unsigned short>(PyDateTime_GET_YEAR(object));
        auto month = static_cast<unsigned char>(PyDateTime_GET_MONTH(object));
        auto day = static_cast<unsigned char>(PyDateTime_GET_DAY(object));
        auto hour = static_cast<unsigned char>(PyDateTime_DATE_GET_HOUR(object));
        auto minute = static_cast<unsigned char>(PyDateTime_DATE_GET_MINUTE(object));
        auto second = static_cast<unsigned char>(PyDateTime_DATE_GET_SECOND(object));
        auto microsecond = static_cast<Int64>(PyDateTime_DATE_GET_MICROSECOND(object));

        LocalDateTime64 local_datetime{year, month, day, hour, minute, second, microsecond};

        return local_datetime.toDateTime64(scale, time_zone);
    }
    catch (const Exception & e)
    {
        raiseConvertionException("datetime64", object, e.displayText());
    }

    UNREACHABLE();
}

Field loadFromPythonObject(PyObject * object, const DataTypePtr & type)
{
    auto type_id = type->getTypeId();
    switch (type_id)
    {
        // case TypeIndex::Nothing = 0:
        //     break;
        case TypeIndex::UInt8:
        {
            if (type->getName() == "bool")
                return loadFromPyBool(object);
            else
                return loadFromPyUnsignedLong<UInt8>(object);
        }
        case TypeIndex::UInt16:
            return loadFromPyUnsignedLong<UInt16>(object);
        case TypeIndex::UInt32:
            return loadFromPyUnsignedLong<UInt32>(object);
        case TypeIndex::UInt64:
            return loadFromPyUnsignedLong<UInt64>(object);
        // case TypeIndex::UInt128:
        //     break;
        // case TypeIndex::UInt256:
        //     break;
        case TypeIndex::Int8:
            return loadFromPyLong<Int8>(object);
        case TypeIndex::Int16:
            return loadFromPyLong<Int16>(object);
        case TypeIndex::Int32:
            return loadFromPyLong<Int32>(object);
        case TypeIndex::Int64:
            return loadFromPyLong<Int64>(object);
        // case TypeIndex::Int128:
        //     break;
        // case TypeIndex::Int256:
        //     break;
        case TypeIndex::Float32:
            return loadFromPyFloat<Float32>(object);
        case TypeIndex::Float64:
            return loadFromPyFloat<Float64>(object);
        case TypeIndex::Date:
            return loadDate16FromPyDate(object);
        case TypeIndex::Date32:
            return loadDate32FromPyDate(object);
        case TypeIndex::DateTime:
            return loadDateTimeFromPyDateTime(object);
        case TypeIndex::DateTime64:
        {
            auto datetime64_type = std::dynamic_pointer_cast<const DataTypeDateTime64>(type);
            return loadDateTime64FromPyDateTime(object, datetime64_type->getScale());
        }
        case TypeIndex::String:
            return loadFromPyUnicode(object);
        case TypeIndex::FixedString:
            return loadFromPyUnicode(object);
        // case TypeIndex::Enum8:
        //     break;
        // case TypeIndex::Enum16:
        //     break;
        // case TypeIndex::Decimal32:
        //     break;
        // case TypeIndex::Decimal64:
        //     break;
        // case TypeIndex::Decimal128:
        //     break;
        // case TypeIndex::Decimal256:
        //     break;
        // case TypeIndex::UUID:
        //     break;
        case TypeIndex::Array:
        {
            if (!PyList_Check(object))
                raiseConvertionException("Array", object);

            Array array;
            size_t list_size = PyList_Size(object);
            auto array_type = std::dynamic_pointer_cast<const DataTypeArray>(type);

            array.reserve(list_size);

            auto nested_type = array_type->getNestedType();
            for (size_t i = 0; i < list_size; i++)
                array.push_back(loadFromPythonObject(PyList_GetItem(object, i), nested_type));

            return array;
        }
        case TypeIndex::Tuple:
        {
            if (!PyTuple_Check(object))
                raiseConvertionException("Tuple", object);

            Tuple tuple;
            size_t tuple_size = PyTuple_Size(object);
            auto tuple_type = std::dynamic_pointer_cast<const DataTypeTuple>(type);

            tuple.reserve(tuple_size);

            for (size_t i = 0; i < tuple_size; i++)
            {
                auto element_type = tuple_type->getElement(i);
                tuple.push_back(loadFromPythonObject(PyTuple_GetItem(object, i), element_type));
            }

            return tuple;
        }
        // case TypeIndex::Set:
        //     break;
        // case TypeIndex::Interval:
        //     break;
        // case TypeIndex::Nullable:
        //     break;
        // case TypeIndex::Function:
        //     break;
        // case TypeIndex::AggregateFunction:
        //     break;
        // case TypeIndex::LowCardinality:
        //     break;
        case TypeIndex::Map:
        {
            if (!PyDict_Check(object))
                raiseConvertionException("Map", object);

            Map map;
            size_t dict_size = PyDict_Size(object);
            auto map_type = std::dynamic_pointer_cast<const DataTypeMap>(type);

            map.reserve(dict_size);

            auto key_type = map_type->getKeyType();
            auto value_type = map_type->getValueType();

            PyObject * key = nullptr;
            PyObject * value = nullptr;
            Py_ssize_t pos = 0;

            while (PyDict_Next(object, &pos, &key, &value))
            {
                if (!key || !value)
                    raiseConvertionException("Map entry", object);

                Tuple pair;
                pair.push_back(loadFromPythonObject(key, key_type));
                pair.push_back(loadFromPythonObject(value, value_type));
                map.push_back(std::move(pair));
            }

            return map;
        }
        // case TypeIndex::Object:
        //     break;
        /// DataType bool is DataTypeNumber<UInt8> with name "bool",
        /// Its typeid is still TypeIndex::UInt8. This branch will not be executed now.
        case TypeIndex::Bool:
            return loadFromPyBool(object);
        case TypeIndex::IPv4:
            return loadFromPyUnsignedLong<IPv4>(object);
        // case TypeIndex::IPv6:
        //     break;
        default:
            PyObject * repr = PyObject_Repr(object);
            throw Exception(
                ErrorCodes::UDF_INTERNAL_ERROR, "Failed to convert Python Object {} to {}", PyUnicode_AsUTF8(repr), type->getName());
    }
}

void PythonListToColumn(IColumn & column, PyObject * py_list, const DataTypePtr & datatype)
{
    if (!PyList_Check(py_list))
        raiseConvertionException("list", py_list);

    size_t size = PyList_Size(py_list);

    column.reserve(column.size() + size);

    auto append_pylist_to_column = [&](PyObject * list) {
        size_t length = PyList_Size(list);
        for (size_t i = 0; i < length; i++)
        {
            PyObject * item = PyList_GetItem(list, i);
            Field field = loadFromPythonObject(item, datatype);
            column.insert(std::move(field));
        }
    };

    append_pylist_to_column(py_list);
}


PyObjectPtr convertColumnToPythonList(const ColumnWithTypeAndName & column_with_type)
{
    const auto & column = column_with_type.column;
    const auto & type = column_with_type.type;
    return convertColumnToPythonList(*column, type, 0, column->size());
}

PyObjectPtr convertColumnToPythonList(const IColumn & column, const DataTypePtr & type, UInt64 offset, UInt64 size)
{
    return columnToPythonList(column, type, offset, size);
}

ColumnPtr convertPythonListToColumn(const PyObjectPtr & py_list, const DataTypePtr & type)
{
    auto column = type->createColumn();
    PythonListToColumn(*column, py_list.get(), type);
    return column;
}

void convertPythonListToColumn(IColumn & column, const PyObjectPtr & py_list, const DataTypePtr & type)
{
    PythonListToColumn(column, py_list.get(), type);
}

void insertPythonObjectToColumn(IColumn & column, const PyObjectPtr & py_object, const DataTypePtr & type)
{
    Field field = loadFromPythonObject(py_object.get(), type);
    column.insert(std::move(field));
}

#if USE_NUMPY

namespace
{
template <typename T>
PyObject * columnToNumpyArray(const IColumn & col, NPY_TYPES npy_type)
{
    const auto & internal_data = assert_cast<const T &>(col).getData();
    npy_intp size = col.size();
    npy_intp dims[1] = {size};
    PyObject * py_array = PyArray_SimpleNewFromData(1, dims, npy_type, (void *)internal_data.data());
    return py_array;
}

template <typename column_type, typename element_type>
void numpyToColumn(PyObject * py_array, IColumn & column)
{
    auto & internal_data = assert_cast<column_type &>(column).getData();
    auto np_array = reinterpret_cast<PyArrayObject *>(py_array);
    npy_intp size = PyArray_SIZE(np_array);
    auto data_raw_ptr = static_cast<element_type *>(PyArray_DATA(np_array));
    internal_data.insert(data_raw_ptr, data_raw_ptr + size);
}
}

PyObject * convertColumnToNumpyArray(const IColumn & column)
{
    auto res = PyArray_ImportNumPyAPI();
    if (res == -1)
    {
        throw Exception(ErrorCodes::UDF_INTERNAL_ERROR, "import numpy failed");
    }

    auto arg_type_id = column.getDataType();
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
        case TypeIndex::String:
        {
            const auto & column_str = assert_cast<const ColumnString &>(column);
            npy_intp size = column_str.size();
            npy_intp dims[1] = {size};
            PyObject * np_array = PyArray_SimpleNew(1, dims, NPY_OBJECT);
            for (npy_intp i = 0; i < size; i++)
            {
                PyObject * item = PyUnicode_FromString(column_str.getDataAt(i).data);
                if (item == nullptr)
                {
                    Py_XDECREF(np_array);
                    throw Exception(ErrorCodes::UDF_INTERNAL_ERROR, "Failed to convert string to Python unicode");
                }
                char * itemptr = reinterpret_cast<char *>(PyArray_GetPtr((PyArrayObject *)np_array, &i));
                PyArray_SETITEM((PyArrayObject *)np_array, itemptr, item);
            }
            return np_array;
        }
        case TypeIndex::FixedString:
        {
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
                std::string error_message = getExceptionMessage();
                throw Exception(ErrorCodes::UDF_INTERNAL_ERROR, "Create fixed string pattern failed, detail message : {}", error_message);
            }

            PyObject * np_array
                = PyArray_NewFromDescr(&PyArray_Type, descr, 1, dims, NULL, (void *)column_fixed.getChars().data(), NPY_ARRAY_CARRAY, NULL);

            return np_array;
        }
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Python UDF does not support data type: {}", column.getDataType());
    }
    UNREACHABLE();
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
ALWAYS_INLINE PyObject * convertColumnToNumpyArray(const ColumnWithTypeAndName & column_with_type)
{
    const auto & column = column_with_type.column;
    return convertColumnToNumpyArray(*column);
}

void covertNumpyArrayToColumn(PyObject * py_array, IColumn & column)
{
    auto res = PyArray_ImportNumPyAPI();
    if (res == -1)
        throw Exception(ErrorCodes::UDF_INTERNAL_ERROR, "import numpy failed");

    auto type = column.getDataType();
    switch (type)
    {
        case TypeIndex::UInt8:
            return numpyToColumn<ColumnUInt8, UInt8>(py_array, column);
        case TypeIndex::UInt16:
            return numpyToColumn<ColumnUInt16, UInt16>(py_array, column);
        case TypeIndex::UInt32:
            return numpyToColumn<ColumnUInt32, UInt32>(py_array, column);
        case TypeIndex::UInt64:
            return numpyToColumn<ColumnUInt64, UInt64>(py_array, column);
        case TypeIndex::Int8:
            return numpyToColumn<ColumnInt8, Int8>(py_array, column);
        case TypeIndex::Int16:
            return numpyToColumn<ColumnInt16, Int16>(py_array, column);
        case TypeIndex::Int32:
            return numpyToColumn<ColumnInt32, Int32>(py_array, column);
        case TypeIndex::Int64:
            return numpyToColumn<ColumnInt64, Int64>(py_array, column);
        case TypeIndex::Float32:
            return numpyToColumn<ColumnFloat32, Float32>(py_array, column);
        case TypeIndex::Float64:
            return numpyToColumn<ColumnFloat64, Float64>(py_array, column);
        case TypeIndex::IPv4:
            return numpyToColumn<ColumnIPv4, IPv4>(py_array, column);
        case TypeIndex::Date:
            return numpyToColumn<ColumnDate, UInt16>(py_array, column);
        case TypeIndex::Date32:
            return numpyToColumn<ColumnDate32, UInt32>(py_array, column);
        case TypeIndex::DateTime:
            return numpyToColumn<ColumnDateTime, UInt32>(py_array, column);
        // case TypeIndex::DateTime64:
        // {
        //     numpyToColumn<ColumnDateTime64, Int64>(py_array, column);
        //     break;
        // }
        case TypeIndex::String:
        case TypeIndex::FixedString:
        {
            auto np_array = reinterpret_cast<PyArrayObject *>(py_array);
            npy_intp size = PyArray_SIZE(np_array);
            for (npy_intp i = 0; i < size; i++)
            {
                PyObject * item = PyArray_GETITEM(np_array, reinterpret_cast<char *>(PyArray_GETPTR1(np_array, i)));
                const char * str = PyUnicode_AsUTF8(item);
                column.insertData(str, strlen(str));
            }
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
#endif

}
}
