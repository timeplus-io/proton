#pragma once

#include <cstdint>

#include <magic_enum.hpp>

namespace cluster::protocol
{
/// Protocol for metadata operations
enum class OpCode : uint16_t
{
    Null = 0,
    Error = 2, /// Error response

    /// DML
    InsertData = 60,
    AlterData = 61,
    AlterPartition = 62,
    Truncate = 63,
    RebuildIndex = 64,
    DeleteRows = 65,
    Optimize = 66,
    DropIndex = 67,

    /// Stream DDL / DML
    CreateStream = 70,
    DeleteStream = 71,
    TruncateStream = 72,
    AlterStreamSettings = 73,
    RenameStream = 74,
    ListStreams = 75,
    AlterStreamSchema = 77,
    GetStream = 78,

    /// Column Metadata CRUD
    CreateColumn = 80,
    DeleteColumn = 81,
    AlterColumn = 82,

    /// Database CRUD
    CreateDatabase = 90,
    DeleteDatabase = 91,
    ListDatabases = 92,
    GetDatabase = 93,
    AlterDatabase = 94,

    /// Functions CRUD
    CreateFunction = 100,
    DeleteFunction = 101,
    AlterFunction = 102,
    ListFunctions = 103,
    GetFunction = 104,

    /// Users CRUD
    CreateUser = 110,
    DeleteUser = 111,
    AlterUser = 112,
    ListUsers = 113,

    /// Roles CRUD
    CreateRole = 120,
    DeleteRole = 121,
    AlterRole = 122,
    ListRoles = 123,

    /// Profiles CRUD
    CreateProfile = 130,
    DeleteProfile = 131,
    AlterProfile = 132,
    ListProfiles = 133,

    /// Quotas CRUD
    CreateQuota = 140,
    DeleteQuota = 141,
    AlterQuota = 142,
    ListQuotas = 143,

    /// Row polices CRUD
    CreateRowPolicy = 150,
    DeleteRowPolicy = 151,
    AlterRowPolicy = 152,
    ListRowPolicies = 153,

    /// ACL
    Grant = 160,
    Revoke = 161,

    /// User Defined Function CURD
    CreateUserDefinedFunction = 180,
    DeleteUserDefinedFunction = 181,
    AlterUserDefinedFunction = 182,
    ListUserDefinedFunctions = 183,
    GetUserDefinedFunction = 184,

    /// Format Schema CRUD
    CreateFormatSchema = 190,
    DeleteFormatSchema = 191,
    AlterFormatSchema = 192,
    ListFormatSchemas = 193,
    GetFormatSchema = 194,

    /// System Command
    DropFormatSchemaCache = 203,
    ChangeLogLevel = 204,
    PipPythonPackage = 205,

    /// Assign a MV to a work node to execute
    AssignMaterializedView = 214,

    /// Access Entity
    CreateAccessEntity = 230,
    DeleteAccessEntity = 231,
    AlterAccessEntity = 232,
    ListAccessEntities = 233,

    /// Disk
    CreateDisk = 240,
    DeleteDisk = 241,
    UpdateDisk = 242,
    ListDisks = 243,

    /// Storage Policy
    CreateStoragePolicy = 250,
    DeleteStoragePolicy = 251,
    UpdateStoragePolicy = 252,
    ListStoragePolicies = 253,

    /// Alert CRUD
    CreateAlert = 260,
    DeleteAlert = 261,
    AlterAlert = 262,
    ListAlerts = 263,
    GetAlert = 264,

    /// Task CRUD
    CreateTask = 270,
    DeleteTask = 271,
    AlterTask = 272,
    ListTasks = 273,
    GetTask = 274,

    /// Named Collection CRUD
    CreateNamedCollection = 280,
    DeleteNamedCollection = 281,
    ListNamedCollections = 282,
    GetNamedCollection = 283,
};

/// Calculate request header version according to request opcode and request version
uint16_t requestHeaderVersion(protocol::OpCode request_opcode, uint16_t request_version);

bool isRequestVersionSupported(protocol::OpCode request_opcode, uint16_t request_version);

void checkRequestVersionSupported(protocol::OpCode request_opcode, uint16_t request_version);

/// Calculate response header version according to request opcode and request version
uint16_t responseHeaderVersion(protocol::OpCode request_opcode, uint16_t request_version);

}

/// https://github.com/Neargye/magic_enum/blob/v0.9.5/doc/limitations.md
/// below range size setting is random number
/// adjusting the range for an enum affects the internal compile-time logic of magic_enum
/// which will increase 1. compile-time 2. result binary size
template <>
struct magic_enum::customize::enum_range<cluster::protocol::OpCode>
{
    static constexpr int min = std::numeric_limits<uint16_t>::min();
    static constexpr int max = 512;
    /// (max - min) must be less than UINT16_MAX.
};
