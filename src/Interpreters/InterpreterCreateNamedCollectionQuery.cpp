#include <Interpreters/InterpreterCreateNamedCollectionQuery.h>

#include <Parsers/ASTCreateNamedCollectionQuery.h>
#include <Access/ContextAccess.h>
#include <Interpreters/Context.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>


namespace DB
{

BlockIO InterpreterCreateNamedCollectionQuery::execute()
{
    auto current_context = getContext();
    current_context->checkAccess(AccessType::CREATE_NAMED_COLLECTION);

    const auto & query = query_ptr->as<const ASTCreateNamedCollectionQuery &>();
    /// proton: starts. FIXME. support it in cluster
    /*if (!query.cluster.empty())
    {
        DDLQueryOnClusterParams params;
        return executeDDLQueryOnCluster(query_ptr, current_context, params);
    }*/
    /// proton: ends

    NamedCollectionFactory::instance().createFromSQL(query);
    return {};
}

}
