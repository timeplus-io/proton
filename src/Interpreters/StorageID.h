#pragma once
#include <base/types.h>
#include <Core/UUID.h>
#include <tuple>
#include <Parsers/IAST_fwd.h>
#include <Core/QualifiedTableName.h>
#include <Common/Exception.h>

/// proton : starts
#include <Cluster/Common/StreamShard.h>
/// proton : ends

namespace Poco
{
namespace Util
{
class AbstractConfiguration;
}
}

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_STREAM;
}

static constexpr char const * TABLE_WITH_UUID_NAME_PLACEHOLDER = "_";

class ASTQueryWithTableAndOutput;
class ASTTableIdentifier;
class Context;

// TODO(ilezhankin): refactor and merge |ASTTableIdentifier|
struct StorageID
{
    String database_name;
    String table_name;
    UUID uuid = UUIDHelpers::Nil;

    StorageID(const String & database, const String & table, UUID uuid_ = UUIDHelpers::Nil)
        : database_name(database), table_name(table), uuid(uuid_)
    {
        assertNotEmpty();
    }

    StorageID(const ASTQueryWithTableAndOutput & query);
    StorageID(const ASTTableIdentifier & table_identifier_node);
    StorageID(const ASTPtr & node);

    String getDatabaseName() const;

    String getTableName() const;

    String getFullTableName() const;
    String getFullNameNotQuoted() const;

    String getNameForLogs() const;

    explicit operator bool () const
    {
        return !empty();
    }

    bool empty() const
    {
        return table_name.empty() && !hasUUID();
    }

    bool hasUUID() const
    {
        return uuid != UUIDHelpers::Nil;
    }

    bool hasDatabase() const { return !database_name.empty(); }

    bool operator<(const StorageID & rhs) const;
    bool operator==(const StorageID & rhs) const;

    void assertNotEmpty() const
    {
        // Can be triggered by user input, e.g. SELECT joinGetOrNull('', 'num', 500)
        if (empty())
            throw Exception(ErrorCodes::UNKNOWN_STREAM, "Both stram name and UUID are empty");
        if (table_name.empty() && !database_name.empty())
            throw Exception(ErrorCodes::UNKNOWN_STREAM, "Stream name is empty, but database name is not");
    }

    /// Avoid implicit construction of empty StorageID. However, it's needed for deferred initialization.
    static StorageID createEmpty() { return {}; }

    QualifiedTableName getQualifiedName() const { return {database_name, getTableName()}; }

    static StorageID fromDictionaryConfig(const Poco::Util::AbstractConfiguration & config,
                                          const String & config_prefix);

    /// If dictionary has UUID, then use it as dictionary name in ExternalLoader to allow dictionary renaming.
    /// ExternalDictnariesLoader::resolveDictionaryName(...) should be used to access such dictionaries by name.
    String getInternalDictionaryName() const { return getShortName(); }
    /// Get short, but unique, name.
    String getShortName() const;

    /// proton : starts
    cluster::StreamShard streamShard(uint32_t shard) const { return cluster::StreamShard{database_name, table_name, uuid, shard}; }

    cluster::Stream stream() const { return cluster::Stream{database_name, table_name, uuid}; }
    /// proton : ends

private:
    StorageID() = default;
};

}

namespace fmt
{
template <>
struct formatter<DB::StorageID>
{
    static constexpr auto parse(format_parse_context & ctx) { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const DB::StorageID & storage_id, FormatContext & ctx) const
    {
        return fmt::format_to(ctx.out(), "{}", storage_id.getNameForLogs());
    }
};
}
