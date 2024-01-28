#include <Common/FieldVisitorToString.h>
#include <Common/typeid_cast.h>

#include <DataTypes/DataTypeCustomSimpleAggregateFunction.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeFactory.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectWithUnionQuery.h>

#include <boost/algorithm/string/join.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int PARAMETERS_TO_AGGREGATE_FUNCTIONS_MUST_BE_LITERALS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int LOGICAL_ERROR;
}

void DataTypeCustomSimpleAggregateFunction::checkSupportedFunctions(const AggregateFunctionPtr & function)
{
    /// TODO Make it sane.
    static const std::vector<String> supported_functions{"any", "any_last", "min",
        "max", "sum", "sum_with_overflow", "group_bit_and", "group_bit_or", "group_bit_xor",
        "sum_map", "min_map", "max_map", "group_array_array", "group_uniq_array_array",
        "sum_mapped_arrays", "min_mapped_arrays", "max_mapped_arrays"};

    // check function
    if (std::find(std::begin(supported_functions), std::end(supported_functions), function->getName()) == std::end(supported_functions))
    {
        throw Exception("Unsupported aggregate function " + function->getName() + ", supported functions are " + boost::algorithm::join(supported_functions, ","),
                ErrorCodes::BAD_ARGUMENTS);
    }
}

String DataTypeCustomSimpleAggregateFunction::getName() const
{
    WriteBufferFromOwnString stream;
    stream << "simple_aggregate_function(" << function->getName();

    if (!parameters.empty())
    {
        stream << "(";
        for (size_t i = 0; i < parameters.size(); ++i)
        {
            if (i)
                stream << ", ";
            stream << applyVisitor(FieldVisitorToString(), parameters[i]);
        }
        stream << ")";
    }

    for (const auto & argument_type : argument_types)
        stream << ", " << argument_type->getName();

    stream << ")";
    return stream.str();
}


static std::pair<DataTypePtr, DataTypeCustomDescPtr> create(const ASTPtr & arguments/* proton: starts */, bool compatible_with_clickhouse = false/* proton: ends */)
{
    String function_name;
    AggregateFunctionPtr function;
    DataTypes argument_types;
    Array params_row;

    if (!arguments || arguments->children.empty())
        throw Exception("Data type simple_aggregate_function requires parameters: "
                        "name of aggregate function and list of data types for arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    if (const ASTFunction * parametric = arguments->children[0]->as<ASTFunction>())
    {
        if (parametric->parameters)
            throw Exception("Unexpected level of parameters to aggregate function", ErrorCodes::SYNTAX_ERROR);
        function_name = parametric->name;

        if (parametric->arguments)
        {
            const ASTs & parameters = parametric->arguments->as<ASTExpressionList &>().children;
            params_row.resize(parameters.size());

            for (size_t i = 0; i < parameters.size(); ++i)
            {
                const ASTLiteral * lit = parameters[i]->as<ASTLiteral>();
                if (!lit)
                    throw Exception(
                        ErrorCodes::PARAMETERS_TO_AGGREGATE_FUNCTIONS_MUST_BE_LITERALS,
                        "Parameters to aggregate functions must be literals. "
                        "Got parameter '{}' for function '{}'",
                        parameters[i]->formatForErrorMessage(),
                        function_name);

                params_row[i] = lit->value;
            }
        }
    }
    else if (auto opt_name = tryGetIdentifierName(arguments->children[0]))
    {
        function_name = *opt_name;
    }
    else if (arguments->children[0]->as<ASTLiteral>())
    {
        throw Exception("Aggregate function name for data type simple_aggregate_function must be passed as identifier (without quotes) or function",
                        ErrorCodes::BAD_ARGUMENTS);
    }
    else
        throw Exception("Unexpected AST element passed as aggregate function name for data type simple_aggregate_function. Must be identifier or function.",
                        ErrorCodes::BAD_ARGUMENTS);

    for (size_t i = 1; i < arguments->children.size(); ++i)
        argument_types.push_back(DataTypeFactory::instance().get(arguments->children[i], compatible_with_clickhouse));

    if (function_name.empty())
        throw Exception("Logical error: empty name of aggregate function passed", ErrorCodes::LOGICAL_ERROR);

    AggregateFunctionProperties properties;
    function = AggregateFunctionFactory::instance().get(function_name, argument_types, params_row, properties);

    DataTypeCustomSimpleAggregateFunction::checkSupportedFunctions(function);

    DataTypePtr storage_type = DataTypeFactory::instance().get(argument_types[0]->getName(), compatible_with_clickhouse);

    if (!function->getReturnType()->equals(*removeLowCardinality(storage_type)))
    {
        throw Exception("Incompatible data types between aggregate function '" + function->getName() + "' which returns " + function->getReturnType()->getName() + " and column storage type " + storage_type->getName(),
                        ErrorCodes::BAD_ARGUMENTS);
    }

    DataTypeCustomNamePtr custom_name = std::make_unique<DataTypeCustomSimpleAggregateFunction>(function, argument_types, params_row);

    return std::make_pair(storage_type, std::make_unique<DataTypeCustomDesc>(std::move(custom_name), nullptr));
}

void registerDataTypeDomainSimpleAggregateFunction(DataTypeFactory & factory)
{
    factory.registerDataTypeCustom("simple_aggregate_function", create);

    factory.registerClickHouseAlias("SimpleAggregateFunction", "simple_aggregate_function");
}

}
