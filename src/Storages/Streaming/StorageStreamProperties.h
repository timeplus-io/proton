#pragma once

#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTCreateQuery.h>

namespace DB
{
struct StreamSettings;
class ColumnsDescription;

struct StorageStreamProperties;
using StorageStreamPropertiesPtr = std::shared_ptr<StorageStreamProperties>;

struct StorageStreamProperties
{
    Int32 replication_factor = 1;
    Int32 shards = 1;
    ASTPtr sharding_expr;
    std::unique_ptr<StreamSettings> storage_settings;

    static StorageStreamPropertiesPtr
    create(ASTStorage & storage_def, const ColumnsDescription & columns, ContextPtr local_context);

    static String getVerboseHelp();
};

}
