#pragma once

#include <Columns/IColumn.h>
#include <Core/Types.h>

namespace DB::Streaming
{

TypeIndex getAsofTypeSize(const IColumn & asof_column, size_t & type_size);

/// maps enum values to types
template <typename F>
void callWithType(TypeIndex which, F && f)
{
    switch (which)
    {
        case TypeIndex::UInt8:
            return f(UInt8());
        case TypeIndex::UInt16:
            return f(UInt16());
        case TypeIndex::UInt32:
            return f(UInt32());
        case TypeIndex::UInt64:
            return f(UInt64());
        case TypeIndex::Int8:
            return f(Int8());
        case TypeIndex::Int16:
            return f(Int16());
        case TypeIndex::Int32:
            return f(Int32());
        case TypeIndex::Int64:
            return f(Int64());
        case TypeIndex::Float32:
            return f(Float32());
        case TypeIndex::Float64:
            return f(Float64());
        case TypeIndex::Decimal32:
            return f(Decimal32());
        case TypeIndex::Decimal64:
            return f(Decimal64());
        case TypeIndex::Decimal128:
            return f(Decimal128());
        case TypeIndex::DateTime64:
            return f(DateTime64());
        default:
            break;
    }

    UNREACHABLE();
}

}
