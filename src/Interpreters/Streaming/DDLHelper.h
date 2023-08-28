#pragma once

#include <Interpreters/Context_fwd.h>
#include <NativeLog/Record/Record.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSystemQuery.h>
#include <Common/ProtonCommon.h>

#include <Poco/JSON/Parser.h>

namespace DB
{
namespace Streaming
{
using CallBack = std::function<void()>;
using TTLSettings = std::pair<String, std::vector<std::pair<String, String>>>;

void getAndValidateStorageSetting(
    std::function<String(const String &)> get_setting, std::function<void(const String &, const String &)> handle_setting);
void prepareEngine(ASTCreateQuery & create, ContextPtr ctx);
/// prepare engine settings for REST API call
void prepareEngineSettings(const ASTCreateQuery & create, ContextMutablePtr ctx);
void checkAndPrepareColumns(ASTCreateQuery & create, ContextPtr context);
void prepareOrderByAndPartitionBy(ASTCreateQuery & create);
void checkAndPrepareCreateQueryForStream(ASTCreateQuery & create, ContextPtr context);
void buildColumnsJSON(Poco::JSON::Object & resp_table, const ASTColumns * columns_list);
TTLSettings parseTTLSettings(const String & payload);
nlog::OpCode getAlterTableParamOpCode(const std::unordered_map<std::string, std::string> & queryParams);
nlog::OpCode getOpCodeFromQuery(const ASTAlterQuery & alter);

String getJSONFromCreateQuery(const ASTCreateQuery & create);
String getJSONFromAlterQuery(const ASTAlterQuery & alter);
String getJSONFromSystemQuery(const ASTSystemQuery & system);

bool assertColumnExists(const String & database, const String & table, const String & column, ContextPtr ctx);
void createDWAL(const String & uuid, Int32 shards, Int32 replication_factor, const String & url_parameters, ContextPtr global_context);
void deleteDWAL(const String & uuid, ContextPtr global_context);

int extractErrorCodeFromMsg(const String & err_msg);
}
}
