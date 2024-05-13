#include "InterpreterMaterializedViewCommandQuery.h"

#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/Streaming/ASTMaterializedViewCommandQuery.h>
#include <Storages/Streaming/StorageMaterializedView.h>

namespace DB
{
namespace ErrorCodes
{
extern const int SYNTAX_ERROR;
}

namespace Streaming
{
namespace
{
class MaterialiedViewCommandQuerySource : public DB::ISource
{
public:
    MaterialiedViewCommandQuerySource(
        MaterializedViewCommandType type_, bool sync_, std::vector<std::shared_ptr<StorageMaterializedView>> && mvs_, Block && header)
        : DB::ISource(std::move(header), true, ProcessorID::MaterialiedViewCommandQuerySourceID)
        , type(type_)
        , sync(sync_)
        , mvs(std::move(mvs_))
    {
        addTotalRowsApprox(mvs.size());
    }

    String getName() const override { return "MaterialiedViewCommandQuerySource"; }

    Chunk generate() override
    {
        size_t num_result_queries = mvs.size();
        switch (command_ast.type)
        {
            case MaterializedViewCommandType::Pause: {
                // Pause the materialized view
                for (auto mv : mvs)
                {
                    mv->pause(command_ast.sync);
                    
                }
                break;
            }
            case MaterializedViewCommandType::Resume: {
                // Resume the materialized view
                for (auto mv : mvs)
                    mv->resume(command_ast.sync);
                break;
            }
            case MaterializedViewCommandType::Abort: {
                // Abort the materialized view
                for (auto mv : mvs)
                    mv->abort(command_ast.sync);
                break;
            }
            case MaterializedViewCommandType::Recover: {
                // Recover the materialized view
                for (auto mv : mvs)
                    mv->recover(command_ast.sync);
                break;
            }
        }

        size_t num_rows = columns.empty() ? 0 : columns.front()->size();
        return Chunk(std::move(columns), num_rows);
    }

private:
    MaterializedViewCommandType type;
    bool sync;
    std::vector<std::shared_ptr<StorageMaterializedView>> mvs;
};
}

BlockIO InterpreterMaterializedViewCommandQuery::execute()
{
    auto command_ast = query_ptr->as<Streaming::ASTMaterializedViewCommandQuery &>();
    auto & expr_list = command_ast.mvs->as<ASTExpressionList &>();
    std::vector<std::shared_ptr<StorageMaterializedView>> mvs;
    for (auto & elem : expr_list.children)
    {
        auto & table_ident = elem->as<ASTTableIdentifier &>();
        auto table_id = table_ident.getTableId();
        if (table_id.database_name.empty())
            table_id.database_name = getContext()->getCurrentDatabase();

        auto table = DatabaseCatalog::instance().getTable(table_id, getContext());
        auto * mv = table->as<StorageMaterializedView>();
        if (!mv)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "'{}' is not a materialized view", table_id.getQualifiedName());

        mvs.push_back(mv->shared_from_this());
    }

    Block header{
        {ColumnString::create(), std::make_shared<DataTypeString>(), "status"},
        {ColumnString::create(), std::make_shared<DataTypeString>(), "name"},
        {ColumnVector<UUID>::create(), std::make_shared<DataTypeUUID>(), "uuid"},
        {ColumnVector<UUID>::create(), std::make_shared<DataTypeUUID>(), "inner_query_id"},
    };

    res_io.pipeline = QueryPipeline(
        std::make_shared<MaterialiedViewCommandQuerySource>(command_ast.type, command_ast.sync, std::move(mvs), std::move(header)));

    return res_io;
}
}
}
