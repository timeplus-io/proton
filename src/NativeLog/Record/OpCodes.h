#pragma once

#include <cstdint>

namespace nlog
{
/// Operation codes
enum class OpCode : uint16_t
{
    /// Data
    ADD_DATA_BLOCK = 0,
    ALTER_DATA_BLOCK,

    /// Table Metadata
    CREATE_TABLE,
    DELETE_TABLE,
    TRUNCATE_TABLE,
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

    /// System Command
    SYSTEM_CMD,

    MAX_OPS_CODE,
};
}
