#include <Common/Config/ExternalGrokPatterns.h>

#include <Columns/ColumnMap.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <base/map.h>

#include <re2/re2.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNSUPPORTED_PARAMETER;
}

namespace
{
    class GrokHelper
    {
    public:
        using Element = std::pair<String, String>;
        using Elements = std::vector<Element>;

        GrokHelper(const String & pattern, ContextPtr context_) : grok_patterns(ExternalGrokPatterns::instance(context_)), regex(replaceGrokPatterns(pattern))
        {
            if (!regex.ok())
                throw Exception(ErrorCodes::UNSUPPORTED_PARAMETER, "Invalid pattern: {}", regex.error());

            num_keys = regex.NumberOfCapturingGroups();
        }

        std::vector<Elements> exec(const ColumnString & column) const
        {
            std::vector<Elements> maps; /// [{"key1": "val1", "key2": "val2"}, ...]
            auto rows = column.size();
            maps.resize(rows);

            /// Captures param value
            int num_captures = num_keys + 1;
            re2::StringPiece matches[num_captures];
            for (size_t i = 0; i < rows; ++i)
            {
                const auto & data = column.getDataAt(i).toString();
                auto & row_map = maps[i];
                row_map.reserve(num_keys);

                if (regex.Match(data, 0, data.size(), re2::RE2::Anchor::ANCHOR_BOTH, matches, num_captures))
                {
                    /// Set keys/values
                    for (const auto & [capturing_index, capturing_name] : regex.CapturingGroupNames())
                    {
                        assert(!matches[capturing_index].empty());
                        row_map.emplace_back(capturing_name, matches[capturing_index].ToString());
                    }
                }

                /// Not matched: empty map
            }
            return maps;
        }

    private:
        /// Replace pattern, for example:
        /// "%{DATA} %{USERNAME:user}"
        /// ->
        /// "(?:.*?) (?P<user>[a-zA-Z0-9._-]+)"
        String replaceGrokPatterns(const String & pattern)
        {
            String replaced_pattern(pattern);

            /// %{pattern_name}
            /// %{pattern_name:capture_name}
            static re2::RE2 grok_regex("%\\{(?P<pattern_name>[a-zA-Z0-9_]+)(?:\\:(?P<capture_name>[a-zA-Z0-9_]+))?\\}");
            assert(grok_regex.ok());

            int num_captures = grok_regex.NumberOfCapturingGroups() + 1;
            assert(num_captures == 3);

            re2::StringPiece grok_matches[num_captures];
            std::unordered_set<String> capture_names;
            while (
                grok_regex.Match(replaced_pattern, 0, replaced_pattern.size(), re2::RE2::Anchor::UNANCHORED, grok_matches, num_captures))
            {
                String string_to_replace(grok_matches[0].ToString());
                String pattern_name(grok_matches[1].ToString());
                String capture_name(grok_matches[2].ToString());

                assert(!pattern_name.empty());

                /// Looking for external patterns
                auto grok_pattern = grok_patterns.tryGetPattern(pattern_name);
                if (!grok_pattern.has_value())
                    throw Exception(
                        ErrorCodes::UNSUPPORTED_PARAMETER,
                        "No support pattern name '{}', you can define a new grok pattern.",
                        pattern_name);

                auto replaced_string
                    = capture_name.empty() ? fmt::format("(?:{})", *grok_pattern) : fmt::format("(?P<{}>{})", capture_name, *grok_pattern);
                auto pos = replaced_pattern.find(string_to_replace);
                assert(pos != std::string::npos);
                replaced_pattern.replace(pos, string_to_replace.size(), replaced_string);

                /// Check duplicate capture names
                if (!capture_name.empty())
                {
                    auto [_, inserted] = capture_names.emplace(capture_name);
                    if (!inserted)
                        throw Exception(
                            ErrorCodes::UNSUPPORTED_PARAMETER, "There are duplicate capture names '{}' in pattern.", capture_name);
                }
            }

            return replaced_pattern;
        }

    private:
        const ExternalGrokPatterns & grok_patterns;
        re2::RE2 regex;
        int num_keys = 0;
    };

    DataTypePtr checkAndGetReturnType(const DataTypes & arguments, const String & func_name)
    {
        if (arguments.size() != 2)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function '{}' doesn't match: passed {}, should be 2, syntax: {}(context, parttern)",
                func_name,
                func_name);

        if (!isString(arguments[0]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The first arugment 'context' of function '{}' shall be string", func_name);

        if (!isString(arguments[1]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The first arugment 'pattern' of function '{}' shall be string", func_name);

        return std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>());
    }

    class FunctionGrok : public IFunction
    {
    public:
        static constexpr auto name = "grok";

        FunctionGrok(const String & pattern_, ContextPtr context_) : grok_helper(pattern_, context_) { }

        String getName() const override { return name; }

        size_t getNumberOfArguments() const override { return 2; }

        bool isStateful() const override { return false; }

        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

        bool useDefaultImplementationForNulls() const override { return false; }

        bool useDefaultImplementationForConstants() const override { return true; }

        /// We do not use default implementation for LowCardinality because this is not a pure function.
        /// If used, optimization for LC may execute function only for dictionary, which gives wrong result.
        bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

        DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override { return checkAndGetReturnType(arguments, name); }

        ColumnPtr
        executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
        {
            MutableColumnPtr result{result_type->createColumn()};
            if (input_rows_count == 0)
                return result;

            auto & map_col = assert_cast<ColumnMap &>(*result.get());
            map_col.reserve(input_rows_count);

            auto & nested_column = map_col.getNestedColumn();
            auto & nested_data_column = map_col.getNestedData();
            IColumn::Offsets & offsets = nested_column.getOffsets();

            auto & key_column = nested_data_column.getColumn(0);
            auto & val_column = nested_data_column.getColumn(1);

            assert(arguments.size() == 2);
            const auto & maps = grok_helper.exec(assert_cast<const ColumnString &>(*arguments[0].column.get()));

            for (const auto & map : maps)
            {
                for (const auto & [key, value] : map)
                {
                    key_column.insert(key);
                    val_column.insert(value);
                }

                offsets.push_back(val_column.size());
            }
            return result;
        }

    private:
        GrokHelper grok_helper;
    };

    class GrokOverloadResolver : public IFunctionOverloadResolver, WithContext
    {
    public:
        static FunctionOverloadResolverPtr create(ContextPtr context_) { return std::make_unique<GrokOverloadResolver>(context_); }

        GrokOverloadResolver(ContextPtr context_) : WithContext(context_) { }

        String getName() const override { return FunctionGrok::name; }
        size_t getNumberOfArguments() const override { return 2; }

        FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type) const override
        {
            /// grok(column, pattern)
            assert(arguments.size() == 2);
            const auto * pattern_col = checkAndGetColumn<ColumnConst>(arguments[1].column.get());
            if (!pattern_col)
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "The second argument 'pattern' of function '{}' shall be constant string",
                    getName());

            auto pattern = pattern_col->getDataAt(0).toString();

            return std::make_unique<FunctionToFunctionBaseAdaptor>(
                std::make_shared<FunctionGrok>(pattern, getContext()),
                collections::map<DataTypes>(arguments, [](const auto & elem) { return elem.type; }),
                return_type);
        }

        DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
        {
            return checkAndGetReturnType(arguments, FunctionGrok::name);
        }
    };
}

void registerFunctionGrok(FunctionFactory & factory)
{
    factory.registerFunction<GrokOverloadResolver>(FunctionGrok::name, FunctionFactory::CaseSensitive);
}
}
