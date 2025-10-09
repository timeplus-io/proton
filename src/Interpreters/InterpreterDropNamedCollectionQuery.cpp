#include <Interpreters/InterpreterDropNamedCollectionQuery.h>
#include <Parsers/ASTDropNamedCollectionQuery.h>
#include <Access/ContextAccess.h>
#include <Interpreters/Context.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>


namespace DB
{

BlockIO InterpreterDropNamedCollectionQuery::execute()
{
    auto current_context = getContext();
    current_context->checkAccess(AccessType::DROP_NAMED_COLLECTION);

    const auto & query = query_ptr->as<const ASTDropNamedCollectionQuery &>();
    /// proton: starts. FIXME. support it in cluster
    /*if (!query.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        return executeDDLQueryOnCluster(query_ptr, current_context, params);
    }*/
    /// proton: ends

    NamedCollectionFactory::instance().removeFromSQL(query);
    return {};
}

}
