#include <AggregateFunctions/AggregateFunctionPythonAdapter.h>

#if USE_PYTHON_UDF

#include <CPython/ConvertDatatypes.h>
#include <CPython/GILGuard.h>
#include <CPython/PyObjectPtr.h>
#include <CPython/Utils.h>
#include <CPython/validatePython.h>
#include <Core/DecimalFunctions.h>
#include <Functions/FunctionsConversion.h>
#include <Functions/UserDefined/UDFHelper.h>
#include <Common/ErrorCodes.h>
#include <Common/logger_useful.h>


namespace DB
{
namespace ErrorCodes
{
extern const int AGGREGATE_FUNCTION_THROW;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int CANNOT_CONVERT_TYPE;
extern const int UDF_COMPILE_ERROR;
extern const int UDF_INTERNAL_ERROR;
extern const int UDF_RUNNING_ERROR;
extern const int UNSUPPORTED;
}

PythonAggrFunctionState::PythonAggrFunctionState(
    const cluster::protocol::UserDefinedFunctionDescriptorPtr & udf_desc_, bool is_changelog_input_, const std::string & module_name_)
    : is_changelog_input(is_changelog_input_), udf_desc(udf_desc_), module_name(module_name_)
{
    columns.reserve(udf_desc->arguments.size());

    for (const auto & arg : udf_desc->arguments)
    {
        auto arg_type = DataTypeFactory::instance().get(arg.type);
        auto col = arg_type->createColumn();
        col->reserve(8);
        columns.emplace_back(std::move(col));
    }

    cpython::GILGuard gil_guard;

    auto py_class = cpython::getClass(udf_desc->name, module_name);

    /// create the instance of python class
    py_instance = newInstance(py_class);

    /// get member functions
    py_initialize_func = tryGetAttr(py_instance, "initialize");
    py_process_func = getAttr(py_instance, "process");
    py_finalize_func = getAttr(py_instance, "finalize");
    py_merge_func = tryGetAttr(py_instance, "merge");
    py_serialize_func = tryGetAttr(py_instance, "serialize");
    py_deserialize_func = tryGetAttr(py_instance, "deserialize");
}

PythonAggrFunctionState::~PythonAggrFunctionState()
{
    cpython::GILGuard gil_guard;
    py_deserialize_func.reset();
    py_serialize_func.reset();
    py_merge_func.reset();
    py_finalize_func.reset();
    py_process_func.reset();
    py_initialize_func.reset();
    py_instance.reset();
}

void PythonAggrFunctionState::add(const IColumn ** src_columns, size_t row_num)
{
    assert(columns.size() >= 1);
    size_t num_of_input_columns = columns.size() - 1;

    for (size_t i = 0; i < num_of_input_columns; i++)
        columns[i]->insertFrom(*src_columns[i], row_num);

    /// _tp_delta column
    if (is_changelog_input)
        columns.back()->insert(1);
}

void PythonAggrFunctionState::negate(const IColumn ** src_columns, size_t row_num)
{
    assert(columns.size() >= 1);
    size_t num_of_input_columns = columns.size() - 1;

    for (size_t i = 0; i < num_of_input_columns; i++)
        columns[i]->insertFrom(*src_columns[i], row_num);

    /// _tp_delta column
    if (is_changelog_input)
        columns.back()->insert(-1);
}

void PythonAggrFunctionState::reinitCache()
{
    MutableColumns new_columns;
    new_columns.reserve(columns.size());
    for (const auto & col : columns)
    {
        auto new_col = col->cloneEmpty();
        new_col->reserve(8);
        new_columns.emplace_back(std::move(new_col));
    }

    columns.swap(new_columns);
    assert(columns[0]);
}

AggregateFunctionPythonAdapter::AggregateFunctionPythonAdapter(
    cluster::protocol::UserDefinedFunctionDescriptorPtr && udf_desc_,
    const DataTypes & argument_types_,
    const Array & params_,
    bool is_changelog_input_)
    : IAggregateFunctionHelper<AggregateFunctionPythonAdapter>(
          argument_types_, params_, DataTypeFactory::instance().get(udf_desc_->return_type))
    , udf_desc(std::move(udf_desc_))
    , num_arguments(argument_types_.size())
    , is_changelog_input(is_changelog_input_)
{
    using_numpy = std::dynamic_pointer_cast<cluster::protocol::PythonUserDefinedFunctionPayload>(udf_desc->payload)->using_numpy;
#if !USE_NUMPY
    if (using_numpy)
        throw Exception(ErrorCodes::UNSUPPORTED, "Numpy is not enabled, because Proton is not compiled with numpy library.");
#endif

    auto python_payload = std::dynamic_pointer_cast<cluster::protocol::PythonUserDefinedFunctionPayload>(udf_desc->payload);

    if (!Py_IsInitialized())
        throw Exception(
            ErrorCodes::UDF_RUNNING_ERROR,
            "Python Interpreter is not initialized, please check the python_path configuration, ensure each path is valid");

    cpython::GILGuard gil_guard;

    module_name = cpython::randomModuleName();
    try
    {
        auto main_module = cpython::getOrAddModule(module_name);
        auto py_class = cpython::tryGetAttr(main_module, udf_desc->name);

        if (!py_class)
        {
            auto byte_code = cpython::compile(python_payload->source);
            cpython::executeByteCode(byte_code, module_name);
            Streaming::runPythonUDFInitHook(*python_payload, udf_desc->name, module_name);
        }

        py_class = cpython::getClass(udf_desc->name, module_name);
        /// create the instance of python class
        auto py_instance = cpython::executeObject(py_class);
        auto py_has_customized_emit = cpython::tryGetAttr(py_instance, "has_customized_emit");

        if (py_has_customized_emit)
            has_user_defined_emit_strategy = PyObject_IsTrue(py_has_customized_emit.get());
    }
    catch (...)
    {
        /// A throwing constructor never runs the destructor, which is what
        /// normally unloads the module; unload here so no half-initialized
        /// module survives.
        if (cpython::hasModule(module_name))
            cpython::unloadModule(module_name);
        throw;
    }
}

AggregateFunctionPythonAdapter::~AggregateFunctionPythonAdapter()
{
    cpython::GILGuard gil_guard;
    cpython::unloadModule(module_name);
}

void AggregateFunctionPythonAdapter::add(
    AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, [[maybe_unused]] Arena * arena) const
{
    this->data(place).add(columns, row_num);
}

void AggregateFunctionPythonAdapter::negate(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const
{
    this->data(place).negate(columns, row_num);
}

void AggregateFunctionPythonAdapter::create(AggregateDataPtr __restrict place) const
{
    new (place) Data(udf_desc, is_changelog_input, module_name);
}

void AggregateFunctionPythonAdapter::deserialize(
    AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const
{
    auto & data = this->data(place);
    if (data.py_deserialize_func == nullptr)
        throw Exception(ErrorCodes::UDF_INTERNAL_ERROR, "Python UDF does not have deserialize method");

    std::string state;
    readBinary(state, buf);
    cpython::GILGuard gil_guard;

    /// read the serialized data from the buffer, then using the deserialize method to restore the object
    auto py_data = cpython::PyObjectPtr{PyBytes_FromStringAndSize(state.data(), state.size())};
    auto py_args = cpython::PyObjectPtr{PyTuple_New(1)};
    PyTuple_SetItem(py_args.get(), 0, py_data.release());
    auto py_result = cpython::executeObject(data.py_deserialize_func, py_args);
    /// in case of throwing exception, which will cause memory leak

    if (!py_result)
    {
        std::string error_message = cpython::getExceptionMessage();
        throw Exception(ErrorCodes::UDF_RUNNING_ERROR, "UDF running error, detail message {}", error_message);
    }
}

void AggregateFunctionPythonAdapter::merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const
{
    auto & data = this->data(place);
    auto & other = this->data(rhs);
    if (!data.py_merge_func)
        throw Exception(ErrorCodes::UDF_INTERNAL_ERROR, "Python UDF does not have merge method, not implemented");

    cpython::GILGuard gil_guard;
    auto py_args = cpython::PyObjectPtr{PyTuple_New(1)};
    const auto & py_rhs_instance = other.py_instance;
    Py_INCREF(py_rhs_instance.get());
    PyTuple_SetItem(py_args.get(), 0, py_rhs_instance.get());

    cpython::executeObject(data.py_merge_func, py_args);
}

size_t AggregateFunctionPythonAdapter::getEmitTimes(ConstAggregateDataPtr __restrict place) const
{
    return this->has_user_defined_emit_strategy ? this->data(place).emit_times : 0;
}

void AggregateFunctionPythonAdapter::serialize(
    ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const
{
    auto & data = this->data(place);
    if (!data.py_serialize_func)
        throw Exception(ErrorCodes::UDF_INTERNAL_ERROR, "Python UDF does not have serialize method");

    cpython::GILGuard gil_guard;

    /// call the serialize method(pickle the whole object), then write the serialized data to the buffer
    auto py_result = cpython::executeObject(data.py_serialize_func);

    /// turn the python object to string
    char * serialize_buf = nullptr;
    Py_ssize_t size;
    PyBytes_AsStringAndSize(py_result.get(), &serialize_buf, &size);
    std::string state(serialize_buf, size);
    writeBinary(state, buf);
}

size_t AggregateFunctionPythonAdapter::flush(AggregateDataPtr __restrict place) const
{
    auto & data = this->data(place);

    cpython::GILGuard gil_guard;

    size_t column_size = is_changelog_input ? udf_desc->arguments.size() : udf_desc->arguments.size() - 1;

    /// turn column to python variable
    cpython::PyObjectPtr py_args{PyTuple_New(column_size)};
    for (size_t i = 0; i < column_size; i++)
    {
        cpython::PyObjectPtr py_array;
#if USE_NUMPY
        if (!using_numpy)
            py_array = cpython::convertColumnToPythonList(
                *data.columns[i], DataTypeFactory::instance().get(udf_desc->arguments[i].type), 0, data.columns[i]->size());
        else
            py_array = cpython::convertColumnToNumpyArray(*data.columns[i]);
#else
        py_array = cpython::convertColumnToPythonList(
            *data.columns[i], DataTypeFactory::instance().get(udf_desc->arguments[i].type), 0, data.columns[i]->size());
#endif
        PyTuple_SetItem(py_args.get(), i, py_array.release());
    }

    /// call the process method
    auto py_result = cpython::executeObject(data.py_process_func, py_args);

    /// check emit times
    size_t emit_times = 0;
    if (has_user_defined_emit_strategy && !Py_IsNone(py_result.get()))
    {
        if (PyLong_Check(py_result.get()))
            emit_times = PyLong_AsSize_t(py_result.get());
        else if (PyBool_Check(py_result.get()) && PyObject_IsTrue(py_result.get()))
            emit_times = 1;
        else
            throw Exception(
                ErrorCodes::UDF_INTERNAL_ERROR, "Python UDA has customized emit strategy, but the return value is not an intege or bool");
    }

    data.reinitCache();
    data.emit_times = emit_times;
    return emit_times;
}

void AggregateFunctionPythonAdapter::insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const
{
    auto & data = this->data(place);
    if (has_user_defined_emit_strategy && data.emit_times == 0)
        return;

    cpython::GILGuard gil_guard;

    auto py_result = cpython::executeObject(data.py_finalize_func);

#if USE_NUMPY
    if (!using_numpy)
    {
        if (has_user_defined_emit_strategy)
            cpython::convertPythonListToColumn(to, py_result, DataTypeFactory::instance().get(udf_desc->return_type));
        else
            cpython::insertPythonObjectToColumn(to, py_result, DataTypeFactory::instance().get(udf_desc->return_type));
    }
    else
        cpython::covertNumpyArrayToColumn(py_result, to);
#else
    if (has_user_defined_emit_strategy)
        cpython::convertPythonListToColumn(to, py_result, DataTypeFactory::instance().get(udf_desc->return_type));
    else
        cpython::insertPythonObjectToColumn(to, py_result, DataTypeFactory::instance().get(udf_desc->return_type));
#endif
    data.emit_times = 0;
}

}
#endif
