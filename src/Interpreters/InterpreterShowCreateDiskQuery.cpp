#include <Interpreters/InterpreterShowCreateDiskQuery.h>

#include <Bootstrap/Globals.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/Protocol/DiskDescriptor.h>
#include <Parsers/ASTShowCreateDiskQuery.h>

#include <Access/ContextAccess.h>
#include <Columns/ColumnString.h>
#include <Common/quoteString.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/Context.h>
#include <Processors/Sources/SourceFromSingleChunk.h>


namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_DISK;
}

BlockIO InterpreterShowCreateDiskQuery::execute()
{
    BlockIO res;
    res.pipeline = executeImpl();
    return res;
}

Block InterpreterShowCreateDiskQuery::getSampleBlock() const
{
    return Block{{ColumnString::create(), std::make_shared<DataTypeString>(), "statement"}};
}

QueryPipeline InterpreterShowCreateDiskQuery::executeImpl()
{
    auto * show_query = query_ptr->as<ASTShowCreateDiskQuery>();

    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::SHOW_DISKS);
    getContext()->checkAccess(access_rights_elements);

    constexpr auto default_disk_name = "default";
    const auto disk_name = show_query->getDiskName();

    String create_statement;

    // Handle default disk specially since it's created in memory by timeplusd
    if (disk_name == default_disk_name)
    {
        create_statement = fmt::format("CREATE DISK `default`(type = local, path = '{}')", getContext()->getPath());
    }
    else
    {
        auto disk_res = Globals::getMetaStore().getMetaDB().getDisk(disk_name);
        if (disk_res.hasError() || !disk_res.result)
            throw Exception(ErrorCodes::UNKNOWN_DISK, "Disk '{}' doesn't exist", disk_name);

        create_statement = fmt::format("CREATE DISK {} {}", backQuoteIfNeed(disk_name), disk_res.result->config);
    }

    MutableColumnPtr column = ColumnString::create();
    column->insert(create_statement);

    return QueryPipeline(
        std::make_shared<SourceFromSingleChunk>(Block{{std::move(column), std::make_shared<DataTypeString>(), "statement"}}));
}
}
