#include "StorageStreamProperties.h"

#include <Interpreters/Context.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/MergeTree/MergeTreeSettings.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_NUMBER_OF_COLUMNS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int TYPE_MISMATCH;
}

namespace
{
    ExpressionActionsPtr
    buildShardingKeyExpression(const ASTPtr & sharding_key, ContextPtr context, const NamesAndTypesList & columns, bool project)
    {
        ASTPtr query = sharding_key;
        auto syntax_result = TreeRewriter(context).analyze(query, columns);
        return ExpressionAnalyzer(query, syntax_result, context).getActions(project);
    }
}

String StorageStreamProperties::getVerboseHelp()
{
    using namespace std::string_literals;

    String help = R"(

Syntax for the Stream table engine:

CREATE STREAM [IF NOT EXISTS] [db.]table_name
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
    ...
    INDEX index_name1 expr1 TYPE type1(...) GRANULARITY value1,
    INDEX index_name2 expr2 TYPE type2(...) GRANULARITY value2
) ENGINE = Stream(shards, shard_by_expr)
ORDER BY expr
[PARTITION BY expr]
[PRIMARY KEY expr]
[SAMPLE BY expr]
[TTL expr [DELETE|TO DISK 'xxx'|TO VOLUME 'xxx'], ...]
[SETTINGS name=value, ...]
)";

    return help;
}

StorageStreamPropertiesPtr
StorageStreamProperties::create(ASTStorage & storage_def, const ColumnsDescription & columns, ContextPtr local_context)
{
    StorageStreamPropertiesPtr properties = std::make_shared<StorageStreamProperties>();
    /// Stream(shards, sharding_expr)

    assert(storage_def.engine);
    ASTs empty_engine_args;
    ASTs & engine_args = storage_def.engine->arguments ? storage_def.engine->arguments->children : empty_engine_args;

    if (engine_args.size() != 2)
    {
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "The current Storage requires 2 parameters {}",
            StorageStreamProperties::getVerboseHelp());
    }

    ASTLiteral * shards_ast = engine_args[0]->as<ASTLiteral>();
    if (shards_ast && shards_ast->value.getType() == Field::Types::UInt64)
    {
        properties->shards = static_cast<UInt32>(shards_ast->value.safeGet<UInt64>());
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Shards must be an unsigned integer {}", getVerboseHelp());
    }

    const auto & sharding_key = engine_args[1];
    if (sharding_key)
    {
        auto sharding_expr = buildShardingKeyExpression(sharding_key, local_context, columns.getAllPhysical(), true);
        const Block & block = sharding_expr->getSampleBlock();

        if (block.columns() != 1)
            throw Exception(ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS, "Sharding expression must return exactly one column");

        auto type = block.getByPosition(0).type;

        if (!type->isValueRepresentedByInteger())
            throw Exception(
                ErrorCodes::TYPE_MISMATCH, "Sharding expression has type {}, but should be one of integer type", type->getName());
    }
    else
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Sharding key expression is required {}", getVerboseHelp());
    }

    properties->sharding_expr = engine_args[1];

    properties->storage_settings = std::make_unique<StreamSettings>(local_context->getStreamSettings());
    properties->storage_settings->loadFromQuery(storage_def, local_context); /// NOTE: will rewrite the AST to add immutable settings.

    return properties;
}
}
