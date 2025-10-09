#include <Columns/ColumnObject.h>
#include <DataTypes/DataTypeObject.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Common/register_objects.h>

namespace DB
{

namespace ErrorCodes
{
extern const int INCORRECT_DATA;
}

namespace
{

class ExecutableFunctionJSONCast final : public IExecutableFunction
{
public:
    static constexpr auto * name = "json_cast";

    explicit ExecutableFunctionJSONCast(DataTypePtr result_type_) : result_type(std::move(result_type_)) { }

    String getName() const override { return name; }

    bool useDefaultImplementationForNulls() const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto res = result_type->createColumn();
        if (input_rows_count == 0)
            return res;

        auto & res_col = typeid_cast<ColumnObject &>(*res);

        auto & typed_paths = res_col.getTypedPaths();
        for (const auto & col : arguments)
            typed_paths[col.name]->insertRangeFrom(*col.column->convertToFullColumnIfConst(), 0, input_rows_count);

        /// This is required to make res_col.size() work properly
        for (size_t i = 0; i < input_rows_count; ++i)
            /// Shared data is not used in this case, thus the shared paths size is always 0
            res_col.getSharedDataOffsets().push_back(0);

        return res;
    }

private:
    DataTypePtr result_type;
};

class FunctionBaseJSONCast final : public IFunctionBase
{
public:
    FunctionBaseJSONCast(DataTypes argument_types_, DataTypePtr return_type_)
        : argument_types(std::move(argument_types_)), return_type(std::move(return_type_))
    {
    }

    String getName() const override { return ExecutableFunctionJSONCast::name; }

    const DataTypes & getArgumentTypes() const override { return argument_types; }

    const DataTypePtr & getResultType() const override { return return_type; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName &) const override
    {
        return std::make_unique<ExecutableFunctionJSONCast>(return_type);
    }

private:
    DataTypes argument_types;
    DataTypePtr return_type;
};

class JSONCastResolver final : public IFunctionOverloadResolver
{
public:
    static constexpr auto * name = ExecutableFunctionJSONCast::name;

    static FunctionOverloadResolverPtr create(ContextPtr) { return std::make_unique<JSONCastResolver>(); }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        std::unordered_map<String, DataTypePtr> typed_paths;
        typed_paths.reserve(arguments.size());

        for (const auto & col : arguments)
        {
            auto [_, inserted] = typed_paths.insert({col.name, col.type});
            /// Follow the same behavior as `cast(..., 'json')` does
            if (!inserted)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot construct json type: Duplicate path found: {}", col.name);
        }

        return std::make_shared<DataTypeObject>(DataTypeObject::SchemaFormat::JSON, std::move(typed_paths));
    }

    bool useDefaultImplementationForNulls() const override { return false; }

private:
    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type) const override
    {
        DataTypes argument_types;
        argument_types.reserve(arguments.size());
        for (const auto & argument : arguments)
            argument_types.emplace_back(argument.type);

        return std::make_unique<FunctionBaseJSONCast>(std::move(argument_types), result_type);
    }
};

}

REGISTER_FUNCTION(JSONCast)
{
    factory.registerFunction<JSONCastResolver>({}, FunctionFactory::CaseInsensitive);
}

}
