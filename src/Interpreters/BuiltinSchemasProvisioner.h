#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Common/ThreadPool.h>

#include <optional>


namespace Poco
{
class Logger;

namespace Util
{
class AbstractConfiguration;
}
}

namespace DB
{

class ASTStorage;

class BuiltinSchemasProvisioner : private WithContext
{
public:
    static constexpr auto * CONFIG_PREFIX = "builtin_schemas";

    BuiltinSchemasProvisioner(ContextPtr context_, const Poco::Util::AbstractConfiguration & config);
    ~BuiltinSchemasProvisioner();

private:
    struct Schema
    {
        String name;
        String ddl;
        bool replace = false;

        ASTPtr create_query_ast;
    };

    bool parse(Schema & schema);

    static String schema_names_in_resource[];
    void loadFromResource();

    void provisionThreadFunction();

    void addOtherBuiltinSchemas(const Poco::Util::AbstractConfiguration & config);

private:
    std::list<std::unique_ptr<Schema>> schemas;

    std::atomic<bool> stopped{false};
    std::optional<ThreadFromGlobalPool> provision_thread;

    LoggerPtr logger;
};

}
