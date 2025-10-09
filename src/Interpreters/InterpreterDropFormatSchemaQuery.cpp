#include <Interpreters/Context.h>
#include <Interpreters/InterpreterDropFormatSchemaQuery.h>

#include <Access/ContextAccess.h>
#include <Bootstrap/Globals.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Parsers/ASTDropFormatSchemaQuery.h>


namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_DELETE_FORMAT_SCHEMA;
}

BlockIO InterpreterDropFormatSchemaQuery::execute()
{
    ASTDropFormatSchemaQuery & drop_format_schema_query = query_ptr->as<ASTDropFormatSchemaQuery &>();

    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::DROP_FORMAT_SCHEMA);

    auto current_context = getContext();

    if (!drop_format_schema_query.cluster.empty())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ON CLUSTER is not implemented for DROP FORMAT SCHEMA");

    current_context->checkAccess(access_rights_elements);
    auto timeout_ms = getContext()->getSettingsRef().query_timeout_sec.value * 1000;
    auto delete_format_schema_req = std::make_shared<cluster::DeleteFormatSchemaRequest>(
        drop_format_schema_query.schema_name,
        drop_format_schema_query.schema_type,
        drop_format_schema_query.if_exists,
        current_context->getNodeID(),
        timeout_ms,
        /*request_version=*/2);
    /// proton: starts
    /// Single-instance mode: use MetaStore directly
    auto & metastore = Globals::getMetaStore();
    /// proton: ends
    auto delete_format_schema_resp = metastore.deleteFormatSchema(std::move(delete_format_schema_req));
    if (delete_format_schema_resp->hasError())
    {
        const auto & err = delete_format_schema_resp->error();
        throw Exception(
            ErrorCodes::CANNOT_DELETE_FORMAT_SCHEMA,
            "Failed to delete format schema {} format {}: error_code={} error_message={}",
            drop_format_schema_query.schema_name,
            drop_format_schema_query.schema_type,
            err.error_code,
            err.error_message);
    }

    return {};
}

}
