#pragma once

#include <DistributedWALClient/Record.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Common/ProtonCommon.h>

#include <Poco/JSON/Parser.h>


namespace DB
{
void getAndValidateStorageSetting(
    std::function<String(const String &)> get_setting, std::function<void(const String &, const String &)> handle_setting);
void prepareEngine(ASTCreateQuery & create, ContextPtr ctx);
/// prepare engine settings for REST API call
void prepareEngineSettings(const ASTCreateQuery & create, ContextMutablePtr ctx);
void prepareColumns(ASTCreateQuery & create);
void prepareOrderByAndPartitionBy(ASTCreateQuery & create);
void prepareCreateQueryForDistributedMergeTree(ASTCreateQuery & create);
void buildColumnsJSON(Poco::JSON::Object & resp_table, const ASTColumns * columns_list);
DWAL::OpCode getAlterTableParamOpCode(const std::unordered_map<std::string, std::string> & queryParams);
DWAL::OpCode getOpCodeFromQuery(const ASTAlterQuery & alter);

String getJSONFromCreateQuery(const ASTCreateQuery & create);
String getJSONFromAlterQuery(const ASTAlterQuery & alter);

void waitForDDLOps(Poco::Logger * log, const ContextMutablePtr & ctx, bool force_sync, UInt64 timeout = DEFAULT_DDL_TIMEOUT_MS);
}
