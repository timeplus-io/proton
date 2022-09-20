#pragma once

#include <Interpreters/Context_fwd.h>
#include <NativeLog/Record/Record.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Common/ProtonCommon.h>

#include <Poco/JSON/Parser.h>


namespace DB
{
namespace Streaming
{
using TTLSettings = std::pair<String, std::vector<std::pair<String, String>>>;

void getAndValidateStorageSetting(
    std::function<String(const String &)> get_setting, std::function<void(const String &, const String &)> handle_setting);
void prepareEngine(ASTCreateQuery & create, ContextPtr ctx);
/// prepare engine settings for REST API call
void prepareEngineSettings(const ASTCreateQuery & create, ContextMutablePtr ctx);
void checkAndPrepareColumns(ASTCreateQuery & create);
void prepareOrderByAndPartitionBy(ASTCreateQuery & create);
void checkAndPrepareCreateQueryForStream(ASTCreateQuery & create);
void buildColumnsJSON(Poco::JSON::Object & resp_table, const ASTColumns * columns_list);
TTLSettings parseTTLSettings(const String & payload);
nlog::OpCode getAlterTableParamOpCode(const std::unordered_map<std::string, std::string> & queryParams);
nlog::OpCode getOpCodeFromQuery(const ASTAlterQuery & alter);

String getJSONFromCreateQuery(const ASTCreateQuery & create);
String getJSONFromAlterQuery(const ASTAlterQuery & alter);

void waitForDDLOps(
    Poco::Logger * log, const ContextMutablePtr & ctx, bool force_sync, UInt64 timeout = ProtonConsts::DEFAULT_DDL_TIMEOUT_MS);
}
}
