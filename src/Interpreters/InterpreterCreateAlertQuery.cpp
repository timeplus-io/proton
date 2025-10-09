#include <Interpreters/InterpreterSelectQuery.h>

#include <Access/Common/AccessRightsElement.h>
#include <Access/Common/AccessType.h>
#include <Bootstrap/Globals.h>
#include <Cluster/MetaStore/MetaStore.h>
#include <Cluster/Requests/GetAlertRequest.h>
#include <Cluster/Requests/GetAlertResponse.h>
#include <Cluster/Requests/CreateAlertRequest.h>
#include <Cluster/Requests/CreateAlertResponse.h>
#include <Cluster/Requests/GetUserDefinedFunctionRequest.h>
#include <Cluster/Requests/GetUserDefinedFunctionResponse.h>
#include <Cluster/Protocol/ExistsOperation.h>
#include <Interpreters/AddDefaultDatabaseVisitor.h>
#include <Interpreters/ApplyWithSubqueryVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateAlertQuery.h>
#include <Parsers/ASTCreateAlertQuery.h>
#include <Storages/Alert/StorageAlert.h>

namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_CREATE_ALERT;
extern const int RESOURCE_ALREADY_EXISTS;
extern const int UNKNOWN_DATABASE;
extern const int UNKNOWN_FUNCTION;
extern const int UNSUPPORTED;
}

BlockIO InterpreterCreateAlertQuery::execute()
{
    auto & create_alert_query = query_ptr->as<ASTCreateAlertQuery &>();

    AccessRightsElements access_rights_elements;
    access_rights_elements.emplace_back(AccessType::CREATE_ALERT);

    auto replace_if_exists = create_alert_query.or_replace;
    if (replace_if_exists)
        access_rights_elements.emplace_back(AccessType::DROP_ALERT);

    auto current_context = getContext();
    current_context->checkAccess(access_rights_elements);

    if (create_alert_query.batch_size > 0)
        if (auto kind = create_alert_query.batch_timeout_unit.kind; kind > IntervalKind::Kind::Hour)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Time unit of batch timeout cannot be bigger than HOUR");

    if (create_alert_query.limit_count > 0)
        if (auto kind = create_alert_query.limit_interval_kind.kind; kind > IntervalKind::Kind::Week)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Time unit of batch timeout cannot be bigger than WEEK");

    auto timeout_ms = current_context->getSettingsRef().query_timeout_sec * 1000;
    /// Single-instance mode: use MetaStore directly
    auto & metastore = Globals::getMetaStore();

    {
        auto req = std::make_shared<cluster::GetUserDefinedFunctionRequest>(
            create_alert_query.function_name,
            /*versions_requested_=*/1,
            metastore.nodeID(), 
            /*consistent_read_=*/false,
            /*timeout_ms_=*/timeout_ms,
            /*request_version=*/1);
        auto resp = metastore.getUserDefinedFunction(req);
        if (resp->hasError())
        {
            if (resp->error().isNotFound())
                throw Exception(ErrorCodes::UNKNOWN_FUNCTION, "UDF {} was not found", create_alert_query.function_name);

            throw Exception(
                resp->error().error_code,
                "Failed to verify UDF {}, error={}",
                create_alert_query.function_name,
                resp->error().error_message);
        }

        chassert(resp->data().descs.size() == 1);

        if (auto fn_type = resp->data().descs.front()->type(); fn_type != cluster::protocol::UDFType::Python)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "ALERT can only call Python UDF, but {} is a {} function",
                create_alert_query.function_name,
                cluster::protocol::udf(fn_type));
    }

    auto exists_op = cluster::protocol::ExistsOperation::Throw;
    if (create_alert_query.if_not_exists)
        exists_op = cluster::protocol::ExistsOperation::Ignore;
    else if (create_alert_query.or_replace)
        exists_op = cluster::protocol::ExistsOperation::Replace;

    UInt32 version{1};
    String database;
    if (create_alert_query.database)
    {
        database = create_alert_query.getDatabase();
        if (!DatabaseCatalog::instance().isDatabaseExist(database))
            throw Exception(ErrorCodes::UNKNOWN_DATABASE, "Database {} does not exist", database);
    }
    else
    {
        database = current_context->getCurrentDatabase();
    }

    if (!DatabaseCatalog::instance().getDatabase(database)->canContainMergeTreeTables())
        throw Exception(ErrorCodes::UNSUPPORTED, "Cannot create alerts in database {}", database);

    auto alert_name = create_alert_query.getName();

    DB::UUID id;

    auto get_alert_req = std::make_shared<cluster::GetAlertRequest>(
        database,
        alert_name,
        1,      // versions_requested
        0,      // initiator (single-instance: always 0)
        false,  // consistent_read
        timeout_ms,
        /*request_version=*/1);
    auto resp = metastore.getAlert(get_alert_req);
    if (const auto err = resp->error(); err.hasError())
    {
        if (!err.isNotFound())
            throw Exception(
                ErrorCodes::CANNOT_CREATE_ALERT,
                "Failed to check if alert {} aleady exists in {}: error_code={} error_message={}",
                alert_name,
                database,
                err.error_code,
                err.error_message);

        id = DB::UUIDHelpers::generateV4();
    }
    else
    {
        if (exists_op == cluster::protocol::ExistsOperation::Ignore)
            return {};
        else if (exists_op == cluster::protocol::ExistsOperation::Throw)
            throw Exception(ErrorCodes::RESOURCE_ALREADY_EXISTS, "Alert {} already exists in {}", alert_name, database);

        version = resp->data().descs[0]->data_version;
        id = resp->data().descs[0]->id;
    }

    /// Addd default database.
    ApplyWithSubqueryVisitor().visit(*create_alert_query.select);
    AddDefaultDatabaseVisitor visitor(getContext(), database);
    visitor.visit(*create_alert_query.select);

    auto create_alert_req = std::make_shared<cluster::CreateAlertRequest>(
        id,
        database,
        alert_name,
        create_alert_query.function_name,
        create_alert_query.getSelectQuery(),
        cluster::protocol::AlertDescriptor::PeriodicCount{
            .count = create_alert_query.batch_size,
            .interval = create_alert_query.batch_timeout,
            .interval_kind = create_alert_query.batch_timeout_unit},
        cluster::protocol::AlertDescriptor::PeriodicCount{
            .count = create_alert_query.limit_count,
            .interval = create_alert_query.limit_interval,
            .interval_kind = create_alert_query.limit_interval_kind},
        exists_op,
        current_context->getUserName(),
        version,
        metastore.nodeID(), 
        timeout_ms,
        /*request_version=*/1);

    auto mutable_context = Context::createCopy(current_context);
    auto desc_ptr = std::make_shared<cluster::protocol::AlertDescriptor>(create_alert_req->data().desc);

    /// Create the storage for (full) validation.
    std::ignore = StorageAlert(StorageID(database, alert_name), desc_ptr, StorageAlert::ValidationLevel::Full, mutable_context);

    auto create_alert_resp = metastore.createAlert(std::move(create_alert_req));
    if (create_alert_resp->hasError())
    {
        const auto & err = create_alert_resp->error();
        throw Exception(
            ErrorCodes::CANNOT_CREATE_ALERT,
            "Failed to create alert {} in {}: error_code={} error_message={}",
            alert_name,
            database,
            err.error_code,
            err.error_message);
    }

    return {};
}
}
