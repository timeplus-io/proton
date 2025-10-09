#include <TableFunctions/TableFunctionPythonCall.h>

#if USE_PYTHON_UDF

#include <Bootstrap/Globals.h>
#include <CPython/ConvertDatatypes.h>
#include <CPython/GILGuard.h>
#include <CPython/PyObjectPtr.h>
#include <CPython/Utils.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/Requests/ListUserDefinedFunctionsRequest.h>
#include <DataTypes/DataTypeFactory.h>
#include <Functions/UserDefined/PythonUserDefinedFunction.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/formatAST.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Processors/Sources/NullSource.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/IStorage.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/registerTableFunctions.h>

#include <base/shared_ptr_helper.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int UDF_INTERNAL_ERROR;
extern const int UDF_RUNNING_ERROR;
}

namespace
{
class PythonUDFSink final : public SinkToStorage
{
public:
    PythonUDFSink(const Block & header, cluster::protocol::UserDefinedFunctionDescriptorPtr udf_desc)
        : SinkToStorage(header, ProcessorID::UDFSink)
        , arg_num(udf_desc->arguments.size())
        , result_type(DataTypeFactory::instance().get(udf_desc->return_type))
    {
        if (Py_IsInitialized() == 0)
            throw Exception(
                ErrorCodes::UDF_RUNNING_ERROR,
                "Python Interpreter is not initialized, please check the python_path configuration, ensure each path is valid");

        module_name = getName() + cpython::randomModuleName();
        const auto & python_udf_payload = assert_cast<cluster::protocol::PythonUserDefinedFunctionPayload &>(*udf_desc->payload);

        cpython::GILGuard gil_guard;

        /// load function
        auto byte_code = cpython::compile(python_udf_payload.source);
        cpython::executeByteCode(byte_code, module_name);

        /// get the function object by UDF name
        py_function = cpython::getFunction(udf_desc->name, module_name);
    }

    ~PythonUDFSink() override
    {
        LOG_INFO(getLogger("PythonUDFSink"), "Dtoring module_name={}", module_name);
        if (py_function && Py_IsInitialized())
        {
            cpython::GILGuard gil_guard;
            py_function.reset();
            cpython::unloadModule(module_name);
        }
    }

    String getName() const override { return "PythonUDFSink"; }

protected:
    void consume(Chunk chunk) override
    {
        if (isCancelled())
            return;

        if (chunk.rows() == 0)
            return;

        cpython::GILGuard gil_guard;

        Block blk = getHeader().cloneWithColumns(chunk.detachColumns());
        const auto & columns = blk.getColumnsWithTypeAndName();

        /// step 1: convert the column to python data type
        cpython::PyObjectPtr py_args{PyTuple_New(arg_num)};
        for (size_t i = 0; i < arg_num; i++)
        {
#if USE_NUMPY
            /// if numpy enabled, it depends on the using_numpy flag, if using_numpy, we convert the column to numpy array,else convert to python list
            if (using_numpy)
                auto py_arg = cpython::convertColumnToNumpyArray(columns[i]);
            else
                auto py_arg = cpython::convertColumnToPythonList(columns[i]);
#else
            /// if not using numpy, we convert the column to python list
            auto py_arg = cpython::convertColumnToPythonList(columns[i]);
#endif
            PyTuple_SetItem(py_args.get(), i, py_arg.release());
        }

        /// step 2: call the python function with the arguments
        std::ignore = cpython::executeObject(py_function, py_args);
    }

private:
    size_t arg_num{0};
    DataTypePtr result_type;
    mutable cpython::PyObjectPtr py_function;
    std::string module_name;
};

class StoragePythonUDF final : public shared_ptr_helper<StoragePythonUDF>, public IStorage
{
    friend struct shared_ptr_helper<StoragePythonUDF>;

public:
    ~StoragePythonUDF() override = default;

    std::string getName() const override { return "PythonUDF"; }

    bool isLocal() const override { return false; }
    bool isRemote() const override { return false; }

    bool supportsParallelInsert() const override { return true; }
    bool supportsSubcolumns() const override { return true; }
    bool supportsDynamicSubcolumns() const override { return true; }
    bool squashInsert() const noexcept override { return false; }
    bool prefersLargeBlocks() const override { return false; }

    SinkToStoragePtr write(const ASTPtr &, const StorageMetadataPtr & metadata_snapshot, ContextPtr) override
    {
        return std::make_shared<PythonUDFSink>(metadata_snapshot->getSampleBlock(), udf_desc);
    }

protected:
    StoragePythonUDF(
        const StorageID & table_id_, ColumnsDescription columns_description_, cluster::protocol::UserDefinedFunctionDescriptorPtr udf_desc_)
        : IStorage(table_id_), udf_desc(std::move(udf_desc_))
    {
        StorageInMemoryMetadata metadata_;
        metadata_.setColumns(columns_description_);
        setInMemoryMetadata(metadata_);
    }

private:
    cluster::protocol::UserDefinedFunctionDescriptorPtr udf_desc;

    UInt64 batch_size{0};
    UInt64 batch_timeout_ms{0};
};

cluster::protocol::UserDefinedFunctionDescriptorPtr getUDFDescription(const String & function_name, const ContextPtr & context)
{
    auto timeout_ms = context->getSettingsRef().query_timeout_sec * 1000;

    auto & metastore = Globals::getMetaStore();

    auto udf_req = std::make_shared<cluster::ListUserDefinedFunctionsRequest>(
        function_name, metastore.nodeID(), /*consistent_read_=*/false, timeout_ms, /*request_version=*/2);
    auto udf_resp = metastore.listUserDefinedFunctions(udf_req);

    if (udf_resp->hasError())
        throw Exception(udf_resp->error().error_code, "Failed to load UDF {}, error={}", function_name, udf_resp->error().error_message);

    if (udf_resp->data().descs.empty())
        throw Exception(ErrorCodes::RESOURCE_NOT_FOUND, "UDF {} was not found", function_name);

    if (udf_resp->data().descs.size() > 1)
        throw DB::Exception(ErrorCodes::UDF_INTERNAL_ERROR, "Found multiple UDFs with the same name '{}'", function_name);

    auto udf_desc = udf_resp->data().descs[0];
    if (!udf_desc || !udf_desc->payload)
        throw DB::Exception(ErrorCodes::UDF_INTERNAL_ERROR, "UDF '{}' has no payload", function_name);

    if (udf_desc->type() != cluster::protocol::UDFType::Python)
        throw DB::Exception(ErrorCodes::UDF_INTERNAL_ERROR, "UDF '{}' is not a Python function", function_name);

    return udf_desc;
}
}

void TableFunctionPythonCall::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto * function = ast_function->as<ASTFunction>();
    if (function == nullptr)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "'{}' is not a function", getName());

    const auto & arguments = function->arguments == nullptr ? ASTs{} : function->arguments->children;
    if (arguments.size() != 1)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function '{}' requires 1 argument but got {}", getName(), arguments.size());

    auto function_name
        = checkAndGetLiteralArgument<String>(evaluateConstantExpressionOrIdentifierAsLiteral(arguments[0], context), "function_name");

    udf_desc = getUDFDescription(function_name, context);
}

ColumnsDescription TableFunctionPythonCall::getActualTableStructure(ContextPtr) const
{
    const auto & factory = DataTypeFactory::instance();
    NamesAndTypesList names_and_types;
    for (const auto & arg : udf_desc->arguments)
        names_and_types.emplace_back(arg.name, factory.get(arg.type));
    return ColumnsDescription{std::move(names_and_types)};
}

StoragePtr TableFunctionPythonCall::executeImpl(
    const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    auto columns = getActualTableStructure(context);
    auto res = StoragePythonUDF::create(StorageID(getDatabaseName(), table_name), columns, udf_desc);
    res->startup();
    return res;
}

void registerTableFunctionPythonCall(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionPythonCall>({.documentation = {}, .allow_readonly = false});
}
}

#else
namespace DB
{
class TableFunctionFactory;

void registerTableFunctionPythonCall(TableFunctionFactory &)
{
}
}

#endif
