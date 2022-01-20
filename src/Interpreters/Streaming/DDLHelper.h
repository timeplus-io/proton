#pragma once

#include <DistributedMetadata/CatalogService.h>

#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTCreateQuery.h>

#include <Poco/JSON/Parser.h>


namespace DB
{
void prepareEngine(ASTCreateQuery & create);
void prepareColumns(ASTCreateQuery & create);
void prepareOrderByAndPartitionBy(ASTCreateQuery & create);
void prepareCreateQueryForDistributedMergeTree(ASTCreateQuery & create);
void buildColumnsJSON(Poco::JSON::Object & resp_table, const ASTColumns * columns_list);
DWAL::OpCode getAlterTableParamOpCode(const std::unordered_map<std::string, std::string> & queryParams);
DWAL::OpCode getOpCodeFromQuery(const ASTAlterQuery & alter);

String getJSONFromCreateQuery(const ASTCreateQuery & create);
String getJSONFromAlterQuery(const ASTAlterQuery & alter);
}
