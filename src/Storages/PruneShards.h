#pragma once

#include <Columns/IColumn.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/QueryShard.h>
#include <Storages/StorageSnapshot.h>

namespace Poco
{
class Logger;
}

namespace DB
{

struct SelectQueryInfo;

IColumn::Selector createSelector(const ColumnWithTypeAndName & result, const std::vector<UInt64> & slot_to_shards);

/// Prune unused shards according to \param sharding_key_expr if prune is possible
/// \return pruned shard IDs which are used for query if prune happens, otherwise return all shard IDs
std::vector<UInt64> pruneShards(
    const ExpressionActionsPtr & sharding_key_expr,
    bool sharding_key_is_deterministic,
    const String & sharding_key_column_name,
    ConstStoragePtr storage,
    const StorageSnapshotPtr & storage_snapshot,
    const SelectQueryInfo & query_info,
    const std::vector<UInt64> & all_shards,
    const ContextPtr & context,
    LoggerPtr logger);

QueryMode getQueryMode(ConstStoragePtr storage, const SelectQueryInfo & query_info, const ContextPtr & context);

ShardsWithQueryMode getPrunedShardsWithQueryMode(
    const ExpressionActionsPtr & sharding_key_expr,
    bool sharding_key_is_deterministic,
    const String & sharding_key_column_name,
    ConstStoragePtr storage,
    const StorageSnapshotPtr & storage_snapshot,
    const SelectQueryInfo & query_info,
    const std::vector<UInt64> & all_shards,
    const ContextPtr & context,
    LoggerPtr logger);

}
