#include <DataTypes/convertToClickHouseType.h>

#include <absl/container/flat_hash_map.h>

namespace DB
{

namespace
{
const absl::flat_hash_map<std::string, std::string> type_map
    = {{"void", "Void"},
       {"bool", "Bool"},
       {"int8", "Int8"},
       {"int16", "Int16"},
       {"int32", "Int32"},
       {"int64", "Int64"},
       {"uint8", "UInt8"},
       {"uint16", "UInt16"},
       {"uint32", "UInt32"},
       {"uint64", "UInt64"},
       {"int128", "Int128"},
       {"uint128", "UInt128"},
       {"int256", "Int256"},
       {"uint256", "UInt256"},
       {"float32", "Float32"},
       {"float64", "Float64"},
       {"string", "String"},
       {"json", "JSON"},
       {"fixed_string", "FixedString"},
       {"datetime", "DateTime"},
       {"date", "Date"},
       {"date32", "Date32"},
       {"datetime64", "DateTime64"},
       {"array", "Array"},
       {"nullable", "Nullable"},
       {"tuple", "Tuple"},
       {"enum", "Enum"},
       {"enum8", "Enum8"},
       {"enum16", "Enum16"},
       {"uuid", "UUID"},
       {"ipv4", "IPv4"},
       {"ipv6", "IPv6"},
       {"decimal", "Decimal"},
       {"decimal32", "Decimal32"},
       {"decimal64", "Decimal64"},
       {"decimal128", "Decimal128"},
       {"decimal256", "Decimal256"},
       {"map", "Map"},
       {"low_cardinality", "LowCardinality"},
       {"point", "Point"},
       {"ring", "Ring"},
       {"polygon", "Polygon"},
       {"multipolygon", "MultiPolygon"}};

/// This function converts the type name to uppercase
std::string convertToClickHouseType(const std::string_view input)
{
    std::string result;
    result.reserve(input.size() + 8);

    const size_t input_size = input.size();
    for (size_t i = 0; i < input_size;)
    {
        size_t start = i;

        while (i < input_size && (std::isalnum(input[i]) || input[i] == '_'))
            ++i;

        std::string_view type_name{input.data() + start, i - start};
        if (auto it = type_map.find(type_name); it != type_map.end())
            result += it->second;
        else
            result += type_name;

        /// Processing parameter part: If there are brackets, continue splicing and parsing the content
        if (i < input_size && input[i] == '(')
        {
            result += '(';

            ++i; /// Skip '('

            int bracket_cnt = 1;
            size_t param_start = i;

            while (i < input_size && bracket_cnt > 0)
            {
                if (input[i] == '(')
                    ++bracket_cnt;
                else if (input[i] == ')')
                    --bracket_cnt;

                ++i;
            }

            /// Recursively process the parameters in parentheses
            std::string_view params{input.data() + param_start, i - param_start - 1};
            result += convertToClickHouseType(params);

            result += ')';
        }

        /// Skip other non-alphanumeric characters
        while (i < input_size && !std::isalnum(input[i]) && input[i] != '_')
        {
            result += input[i];
            ++i;
        }
    }

    return result;
}

}

std::string convertToClickHouseType(const std::string & input)
{
    return convertToClickHouseType(std::string_view{input});
}

}
