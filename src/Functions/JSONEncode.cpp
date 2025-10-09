#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>
#include <Processors/Formats/Impl/JSONEachRowRowOutputFormat.h>
#include <Common/register_objects.h>

namespace DB
{

namespace
{

class ExecutableFunctionJSONEncode final : public IExecutableFunction
{
public:
    static constexpr auto name = "json_encode";

    explicit ExecutableFunctionJSONEncode(const FormatSettings & format_settings_) : format_settings(format_settings_)
    {
        /// We don't need the "\n" after each object.
        /// See: JSONEachRowRowOutputFormat::writeRowEndDelimiter
        format_settings.json.array_of_rows = true;
    }

    String getName() const override { return name; }

    bool useDefaultImplementationForNulls() const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto res = ColumnString::create();

        if (arguments.empty())
        {
            res->insertDefault();
            return ColumnConst::create(std::move(res), input_rows_count);
        }

        if (input_rows_count == 0)
            return res;

        ColumnString::Chars & data_to = res->getChars();
        ColumnString::Offsets & offsets_to = res->getOffsets();
        offsets_to.resize(input_rows_count);

        WriteBufferFromVector<ColumnString::Chars> buf(data_to);

        Block header;
        header.reserve(arguments.size());
        for (const auto & arg : arguments)
            header.insert(arg.cloneEmpty());

        JSONEachRowRowOutputFormat format{buf, header, format_settings};

        Columns cols;
        cols.reserve(arguments.size());
        for (const auto & col : arguments)
            /// JSONEachRowRowOutputFormat can't handle ColumnConst
            cols.push_back(col.column->convertToFullColumnIfConst());

        for (size_t row = 0; row < input_rows_count; ++row)
        {
            format.write(cols, row);
            DB::writeChar(0, buf);
            offsets_to[row] = buf.count();
        }

        return res;
    }

private:
    FormatSettings format_settings;
};

class FunctionBaseJSONEncode final : public IFunctionBase
{
public:
    FunctionBaseJSONEncode(DataTypes argument_types_, DataTypePtr return_type_, const FormatSettings & format_settings_)
        : argument_types(std::move(argument_types_)), return_type(std::move(return_type_)), format_settings(format_settings_)
    {
    }

    String getName() const override { return ExecutableFunctionJSONEncode::name; }

    const DataTypes & getArgumentTypes() const override { return argument_types; }

    const DataTypePtr & getResultType() const override { return return_type; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName &) const override
    {
        return std::make_unique<ExecutableFunctionJSONEncode>(format_settings);
    }

private:
    DataTypes argument_types;
    DataTypePtr return_type;
    FormatSettings format_settings;
};

class JSONEncodeResolver final : public IFunctionOverloadResolver, WithContext
{
public:
    static constexpr auto * name = ExecutableFunctionJSONEncode::name;
    static FunctionOverloadResolverPtr create(ContextPtr context_) { return std::make_unique<JSONEncodeResolver>(std::move(context_)); }

    explicit JSONEncodeResolver(ContextPtr context_) : WithContext(context_) { }

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override { return std::make_shared<DataTypeString>(); }

    bool useDefaultImplementationForNulls() const override { return false; }

private:
    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type) const override
    {
        DataTypes argument_types;
        argument_types.reserve(arguments.size());
        for (const auto & argument : arguments)
            argument_types.emplace_back(argument.type);

        return std::make_unique<FunctionBaseJSONEncode>(std::move(argument_types), result_type, getFormatSettings(getContext()));
    }
};

}

REGISTER_FUNCTION(JSONEncode)
{
    factory.registerFunction<JSONEncodeResolver>({}, FunctionFactory::CaseInsensitive);
}

}
