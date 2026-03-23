#include "PostgreSQLProtocol.h"

namespace DB::PostgreSQLProtocol::Messaging
{

ColumnTypeSpec convertTypeIndexToPostgresColumnTypeSpec(TypeIndex type_index)
{
    switch (type_index)
    {
        case TypeIndex::Int8:
            return {ColumnType::CHAR, 1};

        case TypeIndex::UInt8:
        case TypeIndex::Int16:
            return {ColumnType::INT2, 2};

        case TypeIndex::UInt16:
        case TypeIndex::Int32:
            return {ColumnType::INT4, 4};

        case TypeIndex::UInt32:
        case TypeIndex::Int64:
            return {ColumnType::INT8, 8};

        case TypeIndex::UInt64:
        case TypeIndex::Int128:
        case TypeIndex::UInt128:
        case TypeIndex::Int256:
        case TypeIndex::UInt256:
            return {ColumnType::NUMERIC, -1};

        case TypeIndex::Float32:
            return {ColumnType::FLOAT4, 4};
        case TypeIndex::Float64:
            return {ColumnType::FLOAT8, 8};

        case TypeIndex::FixedString:
        case TypeIndex::String:
            return {ColumnType::VARCHAR, -1};

        case TypeIndex::Date:
        case TypeIndex::Date32:
            return {ColumnType::DATE, 4};

        case TypeIndex::DateTime:
            return {ColumnType::TIMESTAMP, 8};

        case TypeIndex::DateTime64:
            return {ColumnType::TIMESTAMPTZ, 8};

        case TypeIndex::Decimal32:
        case TypeIndex::Decimal64:
        case TypeIndex::Decimal128:
        case TypeIndex::Decimal256:
            return {ColumnType::NUMERIC, -1};

        case TypeIndex::UUID:
            return {ColumnType::UUID, 16};

        case TypeIndex::Enum8:
        case TypeIndex::Enum16:
            return {ColumnType::VARCHAR, -1};

        case TypeIndex::Map:
            return {ColumnType::JSONB, -1};

        case TypeIndex::Array:
        case TypeIndex::Tuple:
            return {ColumnType::VARCHAR, -1};

        default:
            return {ColumnType::VARCHAR, -1};
    }
}

}
