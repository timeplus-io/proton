#include <CPython/ConvertDatatypes.h>
#include <CPython/GILGuard.h>
#include <CPython/PyObjectPtr.h>
#include <CPython/Utils.h>
#include <CPython/validatePython.h>
#include <DataTypes/DataTypeFactory.h>
#include <base/scope_guard.h>
#include <Common/Exception.h>

#include <Python.h>

namespace DB::ErrorCodes
{
extern const int AGGREGATE_FUNCTION_THROW;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int NOT_IMPLEMENTED;
extern const int UDF_COMPILE_ERROR;
extern const int UDF_MEMORY_THRESHOLD_EXCEEDED;
extern const int UDF_INTERNAL_ERROR;
extern const int UDF_RETURN_TYPE_ERROR;
extern const int UDF_RUNNING_ERROR;
}

namespace DB::cpython
{
namespace
{
size_t getDimension(const PyObjectPtr & py_list)
{
    size_t depth = 0;
    if (PyList_Check(py_list.get()))
    {
        Py_ssize_t size = PyList_Size(py_list.get());

        if (size > 0)
        {
            PyObjectPtr item{PyList_GetItem(py_list.get(), 0)};
            int item_depth = getDimension(item);

            /// The ownership of the item object belongs to py_list,
            /// so we should not release this object here.
            item.release();

            if (item_depth > depth)
            {
                depth = item_depth;
            }
        }
        depth += 1;
    }
    return depth;
}

void compare(const PyObjectPtr & expected_list, const PyObjectPtr & test_list)
{
    // Ensure the test input is a list
    if (!PyList_Check(test_list.get()))
        throw Exception(ErrorCodes::UDF_RETURN_TYPE_ERROR, "UDF return value must be list, but got {}", getObjectType(test_list));

    size_t expected_dim = getDimension(expected_list);
    size_t test_dim = getDimension(test_list);
    if (expected_dim != test_dim)
    {
        std::string expected_list_str = convertPyObjectToString(expected_list);
        std::string test_list_str = convertPyObjectToString(test_list);
        throw Exception(
            ErrorCodes::UDF_RETURN_TYPE_ERROR,
            "The Python UDF return list shape is invalid, expected a {} dimension list, for example {}, but the actual UDF return list is "
            "a {} dimension list, like {}, please check the return list shape carefully",
            expected_dim,
            expected_list_str,
            test_dim,
            test_list_str);
    }
}

PyObjectPtr prepareArgs(Poco::JSON::Array::Ptr json_arguments)
{
    auto & factory = DataTypeFactory::instance();
    auto py_args = PyObjectPtr{PyTuple_New(json_arguments->size())};

    for (size_t i = 0; i < json_arguments->size(); i++)
    {
        const auto & arg = json_arguments->getObject(i);
        if (arg->has("type"))
        {
            std::string type_name = arg->get("type");
            auto col_type = factory.get(type_name);
            auto col = col_type->createColumn();
            col->insertDefault();
            PyObjectPtr py_arg = cpython::convertColumnToPythonList(*col, col_type, 0, col->size());
            PyTuple_SetItem(py_args.get(), i, py_arg.release());
        }
    }

    return std::move(py_args);
}

PyObjectPtr getExpectedResult(const std::string & return_type)
{
    auto & factory = DataTypeFactory::instance();
    auto data_type = factory.get(return_type);
    auto return_column = data_type->createColumn();
    return_column->insertDefault();

    return PyObjectPtr{convertColumnToPythonList(*return_column, data_type, 0, return_column->size())};
}

void checkInitHookExists(const std::string & init_function_name, const std::string & module_name)
{
    try
    {
        getFunction(init_function_name, module_name);
    }
    catch (Exception &)
    {
        throw Exception(
            ErrorCodes::UDF_INTERNAL_ERROR,
            "The init function '{}' configured via 'init_function_name' is not defined in the UDF source",
            init_function_name);
    }
}
}


void validateAggregationFunctionSource(Poco::JSON::Object::Ptr config, const std::vector<std::string> & required_member_funcs)
{
    cpython::GILGuard gil;
    std::string module_name = randomModuleName();

    auto py_byte_code = compile(config->get("source"));
    auto exe_result = executeByteCode(std::move(py_byte_code), module_name);

    const std::string & function_name = config->getValue<std::string>("name");
    auto py_class = getClass(function_name, module_name);

    if (config->optValue<std::string>("init_function_name", "").empty())
    {
        auto py_instance = newInstance(py_class);

        for (const auto & func_name : required_member_funcs)
            getInstanceMethod(py_instance, func_name);
    }
    else
    {
        /// The class constructor may depend on globals the init hook sets up, and the
        /// hook only runs at execution-time module load (its parameters may come from a
        /// named collection). Check the required methods on the class without
        /// instantiating it.
        for (const auto & func_name : required_member_funcs)
            getInstanceMethod(py_class, func_name);

        checkInitHookExists(config->getValue<std::string>("init_function_name"), module_name);
    }

    if (hasException())
        throw Exception(
            ErrorCodes::UDF_INTERNAL_ERROR,
            "The Python UDF {} failed to execute, the error message is: {}",
            function_name,
            getExceptionMessage());
}

void validateStatelessFunctionSource(Poco::JSON::Object::Ptr config)
{
    cpython::GILGuard gil;
    std::string module_name = randomModuleName();

    auto py_byte_code = compile(config->get("source"));
    executeByteCode(py_byte_code, module_name);
    auto name = config->getValue<std::string>("name");

    /// checking if the function is defined in the module
    getFunction(name, module_name);

    if (auto init_function_name = config->optValue<std::string>("init_function_name", ""); !init_function_name.empty())
        checkInitHookExists(init_function_name, module_name);

    if (hasException())
        throw Exception(
            ErrorCodes::UDF_INTERNAL_ERROR, "The Python UDF {} failed to execute, the error message is: {}", name, getExceptionMessage());
}

}
