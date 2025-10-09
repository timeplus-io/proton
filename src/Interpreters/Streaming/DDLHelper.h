#pragma once

#include <Cluster/SchemaRecord/SchemaRecord.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTCreateQuery.h>
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
void prepareStorageEngine(ASTCreateQuery & create, ContextPtr ctx);
/// prepare engine settings for REST API call
void prepareStorageEngineSettings(const ASTCreateQuery & create, ContextMutablePtr ctx);
void checkAndPrepareColumnsAndIndexes(ASTCreateQuery & create, ContextPtr context);
void prepareOrderByAndPartitionBy(ASTCreateQuery & create);
void checkAndPrepareCreateQueryForStream(ASTCreateQuery & create, ContextPtr context);
void buildColumnsJSON(Poco::JSON::Object & resp_table, const ASTColumns * columns_list);
TTLSettings parseTTLSettings(const String & payload);
cluster::protocol::OpCode getAlterTableParamOpCode(const std::unordered_map<std::string, std::string> & queryParams);
cluster::protocol::OpCode getOpCodeFromQuery(const ASTAlterQuery & alter);

String getJSONFromCreateQuery(const ASTCreateQuery & create);
String getJSONFromAlterQuery(const ASTAlterQuery & alter);

bool assertColumnExists(const String & database, const String & table, const String & column, ContextPtr ctx);

int extractErrorCodeFromMsg(const String & err_msg);
}
}
