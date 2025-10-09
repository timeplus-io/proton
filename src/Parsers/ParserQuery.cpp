#include <Parsers/ParserAlterQuery.h>
#include <Parsers/ParserCreateFunctionQuery.h>
#include <Parsers/ParserBackupQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ParserDropFunctionQuery.h>
#include <Parsers/ParserDropNamedCollectionQuery.h>
#include <Parsers/ParserAlterNamedCollectionQuery.h>
#include <Parsers/ParserDropQuery.h>
#include <Parsers/ParserInsertQuery.h>
#include <Parsers/ParserOptimizeQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/ParserQueryWithOutput.h>
#include <Parsers/ParserRenameQuery.h>
#include <Parsers/ParserSetQuery.h>
#include <Parsers/ParserSystemQuery.h>
#include <Parsers/ParserUseQuery.h>
#include <Parsers/ParserExternalDDLQuery.h>
#include <Parsers/ParserTransactionControl.h>
#include <Parsers/ParserDeleteQuery.h>

#include <Parsers/Access/ParserCreateQuotaQuery.h>
#include <Parsers/Access/ParserCreateRoleQuery.h>
#include <Parsers/Access/ParserCreateRowPolicyQuery.h>
#include <Parsers/Access/ParserCreateSettingsProfileQuery.h>
#include <Parsers/Access/ParserCreateUserQuery.h>
#include <Parsers/Access/ParserDropAccessEntityQuery.h>
#include <Parsers/Access/ParserGrantQuery.h>
#include <Parsers/Access/ParserSetRoleQuery.h>

/// proton: starts
#include <Parsers/ParserCreateAlertQuery.h>
#include <Parsers/ParserCreateDiskQuery.h>
#include <Parsers/ParserCreateFormatSchemaQuery.h>
#include <Parsers/ParserCreateStoragePolicyQuery.h>
#include <Parsers/ParserCreateTaskQuery.h>
#include <Parsers/ParserDropAlertQuery.h>
#include <Parsers/ParserDropDiskQuery.h>
#include <Parsers/ParserDropFormatSchemaQuery.h>
#include <Parsers/ParserDropStoragePolicyQuery.h>
#include <Parsers/ParserDropTaskQuery.h>
/// proton: ends


namespace DB
{


bool ParserQuery::parseImpl(Pos & pos, ASTPtr & node, Expected & expected, [[ maybe_unused ]] bool hint)
{
    ParserQueryWithOutput query_with_output_p(end, allow_settings_after_format_in_insert);
    ParserInsertQuery insert_p(end, allow_settings_after_format_in_insert);
    ParserUseQuery use_p;
    ParserSetQuery set_p;
    ParserSystemQuery system_p;
    ParserCreateUserQuery create_user_p;
    ParserCreateRoleQuery create_role_p;
    ParserCreateQuotaQuery create_quota_p;
    ParserCreateRowPolicyQuery create_row_policy_p;
    ParserCreateSettingsProfileQuery create_settings_profile_p;
    ParserCreateFunctionQuery create_function_p;
    ParserDropFunctionQuery drop_function_p;
    ParserCreateNamedCollectionQuery create_named_collection_p;
    ParserDropNamedCollectionQuery drop_named_collection_p;
    ParserAlterNamedCollectionQuery alter_named_collection_p;
    ParserDropAccessEntityQuery drop_access_entity_p;
    ParserGrantQuery grant_p;
    ParserSetRoleQuery set_role_p;
    ParserExternalDDLQuery external_ddl_p;
    ParserTransactionControl transaction_control_p;
    ParserDeleteQuery delete_p;
    ParserBackupQuery backup_p;
    /// proton: starts
    ParserCreateFormatSchemaQuery create_format_schema_p;
    ParserDropFormatSchemaQuery drop_format_schema_p;
    ParserCreateDiskQuery create_disk_p;
    ParserDropDiskQuery drop_disk_p;
    ParserCreateStoragePolicyQuery create_policy_p;
    ParserDropStoragePolicyQuery drop_policy_p;
    ParserCreateAlertQuery create_alert_p;
    ParserDropAlertQuery drop_alert_p;
    ParserCreateTaskQuery create_task_p;
    ParserDropTaskQuery drop_task_p;
    /// proton: ends

    bool res = query_with_output_p.parse(pos, node, expected)
        || insert_p.parse(pos, node, expected)
        || use_p.parse(pos, node, expected)
        || set_role_p.parse(pos, node, expected)
        || set_p.parse(pos, node, expected)
        || system_p.parse(pos, node, expected)
        || create_user_p.parse(pos, node, expected)
        || create_role_p.parse(pos, node, expected)
        || create_quota_p.parse(pos, node, expected)
        || create_row_policy_p.parse(pos, node, expected)
        || create_settings_profile_p.parse(pos, node, expected)
        || create_function_p.parse(pos, node, expected)
        /// proton: starts
        || create_format_schema_p.parse(pos, node, expected)
        || drop_format_schema_p.parse(pos, node, expected)
        || create_disk_p.parse(pos, node, expected)
        || drop_disk_p.parse(pos, node, expected)
        || create_policy_p.parse(pos, node, expected)
        || drop_policy_p.parse(pos, node, expected)
        || create_alert_p.parse(pos, node, expected)
        || drop_alert_p.parse(pos, node, expected)
        || create_task_p.parse(pos, node, expected)
        || drop_task_p.parse(pos, node, expected)
        /// proton: ends
        || drop_function_p.parse(pos, node, expected)
        || create_named_collection_p.parse(pos, node, expected)
        || drop_named_collection_p.parse(pos, node, expected)
        || alter_named_collection_p.parse(pos, node, expected)
        || drop_access_entity_p.parse(pos, node, expected)
        || grant_p.parse(pos, node, expected)
        || external_ddl_p.parse(pos, node, expected)
        || transaction_control_p.parse(pos, node, expected)
        || delete_p.parse(pos, node, expected)
        || backup_p.parse(pos, node, expected);

    return res;
}

}
