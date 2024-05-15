#ifdef ENABLE_PYTHON_UDF
#include <AggregateFunctions/AggregateFunctionPythonAdapter.h>

#include <CPython/ConvertDatatypes.h>
#include <CPython/validatePython.h>
#include <Core/DecimalFunctions.h>
#include <Functions/FunctionsConversion.h>
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

}


PyObject * getMemberFunctionByName(PyObject * py_instance, const char * member_name, bool is_necessary = true)
{
    if (PyObject_HasAttrString(py_instance, member_name))
    {
        return PyObject_GetAttrString(py_instance, member_name);

    }
    else if (is_necessary)
    {
        throw Exception(ErrorCodes::UDF_INTERNAL_ERROR, "Python UDF does not have {} method", member_name);
    }
    return nullptr;
}

PythonAggrFunctionState::PythonAggrFunctionState(const PythonUserDefinedFunctionConfigurationPtr & config_) : config(config_)
{
    /// no tp_delta
    columns.reserve(config->arguments.size());

    for (const auto & arg : config->arguments)
    {
        auto col = arg.type->createColumn();
        col->reserve(8);
        columns.emplace_back(std::move(col));
    }

    PyGILState_STATE gstate = PyGILState_Ensure();

    PyObject * pyClass = PyObject_GetAttrString(PyImport_AddModule("__main__"), config->name.c_str());
    /// create the instance of python class
    py_instance = PyObject_CallObject(pyClass, nullptr);

    /// get member functions
    py_initialize_func = getMemberFunctionByName(py_instance, "initialize", false);
    py_process_func = getMemberFunctionByName(py_instance, "process");
    py_finalize_func = getMemberFunctionByName(py_instance, "finalize");
    py_merge_func = getMemberFunctionByName(py_instance, "merge", false);
    py_serialize_func = getMemberFunctionByName(py_instance, "serialize", false);
    py_deserialize_func = getMemberFunctionByName(py_instance, "deserialize", false);

    PyGILState_Release(gstate);

}
PythonAggrFunctionState::~PythonAggrFunctionState()
{
    PyGILState_STATE gstate = PyGILState_Ensure();

    Py_XDECREF(py_instance);
    Py_XDECREF(py_initialize_func);
    Py_XDECREF(py_process_func);
    Py_XDECREF(py_finalize_func);
    Py_XDECREF(py_merge_func);
    Py_XDECREF(py_serialize_func);
    Py_XDECREF(py_deserialize_func);
    PyGILState_Release(gstate);

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
    columns.clear();
    for (const auto & arg : config->arguments)
    {
        auto col = arg.type->createColumn();
        col->reserve(8);
        columns.emplace_back(std::move(col));
    }

}


AggregateFunctionPythonAdapter::AggregateFunctionPythonAdapter(
    const PythonUserDefinedFunctionConfigurationPtr & config_,
    const DataTypes & argument_types_,
    const Array & params_,
    bool is_changelog_input_)
    : IAggregateFunctionHelper<AggregateFunctionPythonAdapter>(argument_types_, params_)
    , num_arguments(argument_types_.size())
    , is_changelog_input(is_changelog_input_)
    , config(config_)
    , using_numpy(config->using_numpy)
{
}


void AggregateFunctionPythonAdapter::add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const
{
    this->data(place).add(columns, row_num);
}

void AggregateFunctionPythonAdapter::negate(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const
{
    this->data(place).negate(columns, row_num);
}


void AggregateFunctionPythonAdapter::create(AggregateDataPtr __restrict place) const
{
    new (place) Data(config);
}
void AggregateFunctionPythonAdapter::deserialize(
    AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const
{
}

void AggregateFunctionPythonAdapter::merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const
{
}

void AggregateFunctionPythonAdapter::serialize(
    ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const
{
}

size_t AggregateFunctionPythonAdapter::flush(AggregateDataPtr __restrict place) const
{
    auto & data = this->data(place);
    PyGILState_STATE gstate = PyGILState_Ensure();

    PyObject * py_args = PyTuple_New(num_arguments);
    for (size_t i = 0; i < num_arguments; i++)
    {
        PyObject * py_array = nullptr;
        if (!using_numpy)
            py_array = cpython::convertColumnToPythonList(*data.columns[i], config->arguments[i].type, 0, data.columns[i]->size());
        else
            py_array = cpython::convertColumnToNumpyArray(*data.columns[i]);
        PyTuple_SetItem(py_args, i, py_array);
    }

    PyObject * py_result = PyObject_CallObject(data.py_process_func, py_args);
    Py_XDECREF(py_args);
    if (py_result == nullptr)
    {
        std::string error_message = cpython::catchException();
        PyGILState_Release(gstate);

        throw Exception(ErrorCodes::UDF_RUNNING_ERROR, "UDF running error, detail message {}", error_message);
    }

    Py_XDECREF(py_result);
    data.reinitCache();
    PyGILState_Release(gstate);
    return 0;
}

void AggregateFunctionPythonAdapter::insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const
{
    auto & data = this->data(place);
    PyGILState_STATE gstate = PyGILState_Ensure();
    PyObject * py_result = PyObject_CallObject(data.py_finalize_func, nullptr);

    if (!using_numpy)
        cpython::convertPythonListToColumn(py_result, to, config->result_type);
    else
        cpython::covertNumpyArrayToColumn(py_result, to);
    Py_XDECREF(py_result);
    PyGILState_Release(gstate);
}


void AggregateFunctionPythonAdapter::addBatchLookupTable8(
    size_t row_begin,
    size_t row_end,
    AggregateDataPtr * map,
    size_t place_offset,
    std::function<void(AggregateDataPtr &)> init,
    const UInt8 * key,
    const IColumn ** columns,
    Arena * arena,
    const IColumn * delta_col) const
{
}


}
#endif
