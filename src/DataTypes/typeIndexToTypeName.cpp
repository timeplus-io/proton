#include "typeIndexToTypeName.h"

#include <magic_enum.hpp>

namespace DB
{

String typeIndexToTypeName(TypeIndex type)
{
    switch (type)
    {
        case TypeIndex::LowCardinality:
            return "low_cardinality";
        case TypeIndex::FixedString:
            return "fixed_string";
        case TypeIndex::AggregateFunction:
            return "aggregate_function";
        default:
            return Poco::toLower(std::string(magic_enum::enum_name(type)));
    }
}

}
