#pragma once

#include <Core/QueryProcessingStage.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/StorageSnapshot.h>

namespace DB
{
struct SelectQueryInfo;

ExpressionActionsPtr
buildShardingKeyExpression(const ASTPtr & sharding_key, ContextPtr context, const NamesAndTypesList & columns, bool project);

bool isExpressionActionsDeterministics(const ExpressionActionsPtr & actions);

QueryProcessingStage::Enum getHistoricalQueryProcessingStageRemote(
    SelectQueryInfo & query_info,
    QueryProcessingStage::Enum to_stage,
    const StorageSnapshotPtr & storage_snapshot,
    ContextPtr local_context,
    ConstStoragePtr storage,
    const ExpressionActionsPtr & sharding_key_expr,
    bool sharding_key_is_deterministic,
    const String & sharding_key_column_name,
    const std::vector<UInt64> & all_shards,
    LoggerPtr logger);

}
