#pragma once

namespace DB
{
namespace DWAL
{
/// Operation codes
enum class OpCode : uint8_t
{
    /// Data
    ADD_DATA_BLOCK = 0,
    ALTER_DATA_BLOCK,

    /// Table Metadata
    CREATE_TABLE,
    DELETE_TABLE,
    ALTER_TABLE,

    /// Column Metadata
    CREATE_COLUMN,
    DELETE_COLUMN,
    ALTER_COLUMN,

    /// Database
    CREATE_DATABASE,
    DELETE_DATABASE,

    /// Dictionary
    CREATE_DICTIONARY,
    DELETE_DICTIONARY,

    MAX_OPS_CODE,

    UNKNOWN = 0x3F,
};
}
}

