#include "InterpreterPauseMaterializedViewQuery.h"

#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/Streaming/ASTPauseMaterializedViewQuery.h>
#include <Storages/Streaming/StorageMaterializedView.h>

namespace DB
{
namespace ErrorCodes
{
extern const int SYNTAX_ERROR;
}

namespace Streaming
{
BlockIO InterpreterPauseMaterializedViewQuery::execute()
{
    auto & expr_list = query_ptr->as<Streaming::ASTPauseMaterializedViewQuery>()->mvs->as<ASTExpressionList&>();
    std::vector<StorageMaterializedView *> mvs;
    for (auto & elem : expr_list.children)
    {
        auto & table_ident = elem->as<ASTTableIdentifier&>();
        auto table_id = table_ident.getTableId();
        if (table_id.database_name.empty())
            table_id.database_name = getContext()->getCurrentDatabase();

        auto * mv = DatabaseCatalog::instance().getTable(table_id, getContext())->as<StorageMaterializedView>();
        if (!mv)
            throw Exception(ErrorCodes::SYNTAX_ERROR, "'{}' is not a materialized view", table_id.getQualifiedName());

        mvs.push_back(mv);
    }

    // Pause the materialized view
    for (auto mv : mvs)
        mv->pause();

    return {};
}
}
}
