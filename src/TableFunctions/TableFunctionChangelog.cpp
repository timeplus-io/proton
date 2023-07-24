#include <TableFunctions/TableFunctionChangelog.h>

#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/IStorage.h>
#include <Storages/Streaming/StorageMaterializedView.h>
#include <Storages/Streaming/StorageStream.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/ProtonCommon.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int BAD_ARGUMENTS;
extern const int UNSUPPORTED;
}

namespace Streaming
{
TableFunctionChangelog::TableFunctionChangelog(const String & name_) : TableFunctionProxyBase(name_)
{
    help_message = fmt::format(
        "Function '{}' requires at least 1 parameter. Usage : changelog(stream[, [key_col1[, key_col2, [...]], version_column], "
        "drop_late_rows). "
        "When `drop_late_rows` is true, either primary key columns and version column are implicitly deduced (for example for versioned_vk "
        "stream "
        "or require users to explicitly specify the primary key columns and version column. "
        "Examples: changelog(versioned_kv, true) or changelog(append, key_col1, key_col2, version_column, true)",
        name);
}

void TableFunctionChangelog::parseArguments(const ASTPtr & func_ast, ContextPtr context)
{
    if (func_ast->children.size() != 1)
        throw Exception(help_message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    ASTs asts;

    auto changelog_func_ast = func_ast->clone();
    auto * node = changelog_func_ast->as<ASTFunction>();
    assert(node);

    auto [args, drop_late_rows] = checkAndExtractArguments(node);

    /// First argument is expected to be table
    resolveStorageID(args[0], context);

    /// Prune the first stream argument
    args.erase(args.begin());

    /// Prune the last `drop_late_rows` bool argument
    if (drop_late_rows.has_value())
        args.pop_back();

    node->arguments->children.swap(args);

    /// Calculate column description
    TableFunctionProxyBase::calculateColumnDescriptions(context);

    if (!streaming)
        throw Exception(ErrorCodes::UNSUPPORTED, "`changelog` table function doesn't supports historical stream yet");

    if (Streaming::isChangelogKeyedDataStream(data_stream_semantic))
        throw Exception(ErrorCodes::UNSUPPORTED, "`changelog` table function doesn't support changelog stream yet");

    /// Here we already erased the stream name in the first argument
    /// For versioned_kv stream, only `drop_late_rows` argument is allowed and it is optional
    /// For versioned-kv stream, we don't allow end user specify the key columns / version column for now
    /// since it already has these defined at storage layer. Changing to a different primary key columns / version column
    /// seems having no sense for now
    /// changelog(versioned_kv, [true | false])
    if (storage && (storage->as<StorageStream>() || storage->as<StorageMaterializedView>())
        && isVersionedKeyedDataStream(storage->dataStreamSemantic()) && node->arguments->children.size() >= 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, help_message);

    if (node->arguments->children.empty())
    {
        /// We expect the underlying storage is versioned-kv or changelog-kv
        if (storage
            && ((storage->as<StorageStream>() || storage->as<StorageMaterializedView>())
                && isVersionedKeyedDataStream(storage->dataStreamSemantic())))
        {
        }
        else
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, help_message);
    }

    if (drop_late_rows && *drop_late_rows && (node->arguments->children.size() == 1))
        /// For other stream / query, we expect at least 1 primary key column and 1 version column arg if target stream is not versioned-kv
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, help_message);

    /// Add _tp_delta column
    columns.add(ColumnDescription(ProtonConsts::RESERVED_DELTA_FLAG, DataTypeFactory::instance().get(TypeIndex::Int8)));

    /// Create table func desc
    streaming_func_desc = createStreamingTableFunctionDescription(changelog_func_ast, context);
    streaming_func_desc->func_ctx = drop_late_rows;
}

std::pair<ASTs, std::optional<bool>> TableFunctionChangelog::checkAndExtractArguments(ASTFunction * node) const
{
    /// changelog(stream | subquery, [key_col_1, [key_col2, ..., version_col]], [drop_late_rows])
    const auto & args = node->arguments->children;
    if (args.empty())
        throw Exception(help_message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    /// changelog(stream)
    if (args.size() == 1)
        return {args, {}};

    size_t end_pos = args.size();

    std::optional<bool> drop_late_rows;

    /// Check the last argument
    if (auto * lit = args.back()->as<ASTLiteral>(); lit)
    {
        if (lit->value.getType() != Field::Types::Bool)
            throw Exception(help_message, ErrorCodes::BAD_ARGUMENTS);

        drop_late_rows = lit->value.get<bool>();
        --end_pos;
    }

    for (size_t i = 1; i < end_pos; ++i)
        /// if (!args[i]->as<ASTFunction>() && !args[i]->as<ASTIdentifier>())
        /// We only support identifier for key columns / version column
        if (!args[i]->as<ASTIdentifier>())
            throw Exception(help_message, ErrorCodes::BAD_ARGUMENTS);

    return {std::move(args), drop_late_rows};
}

void registerTableFunctionChangelog(TableFunctionFactory & factory)
{
    factory.registerFunction("changelog", []() -> TableFunctionPtr { return std::make_shared<TableFunctionChangelog>("changelog"); });
}
}
}
