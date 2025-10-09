#include <TableFunctions/TableFunctionRemote.h>

#include <Storages/getStructureOfRemoteTable.h>
#include <Storages/StorageDistributed.h>
#include <Storages/ExternalDataSourceConfiguration.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTExpressionList.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Common/typeid_cast.h>
#include <Common/parseRemoteDescription.h>
#include <Common/Macros.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Core/Defines.h>
#include <TableFunctions/registerTableFunctions.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}


void TableFunctionRemote::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    ASTs & args_func = ast_function->children;
    ExternalDataSourceConfiguration configuration;

    String cluster_description;

    if (args_func.size() != 1)
        throw Exception(help_message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    ASTs & args = args_func.at(0)->children;

    /**
     * Max number of arguments for remote function is 6.
     * For now named collection can be used only for remote as cluster does not require credentials.
     */
    size_t max_args = 6;
    auto named_collection = getExternalDataSourceConfiguration(args, context, false, false);
    if (named_collection)
    {
        /**
         * Common arguments: database, table, username, password, addresses_expr.
         * Specific args (remote): sharding_key, or database (in case it is not ASTLiteral).
         * None of the common arguments is empty at this point, it is checked in getExternalDataSourceConfiguration.
         */
        auto [common_configuration, storage_specific_args, _] = named_collection.value();
        configuration.set(common_configuration);

        for (const auto & [arg_name, arg_value] : storage_specific_args)
        {
            if (arg_name == "sharding_key")
            {
                sharding_key = arg_value;
            }
            else if (arg_name == "database")
            {
                const auto * function = arg_value->as<ASTFunction>();
                if (function && TableFunctionFactory::instance().isTableFunctionName(function->name))
                {
                    remote_table_function_ptr = arg_value;
                }
                else
                {
                    auto database_literal = evaluateConstantExpressionOrIdentifierAsLiteral(arg_value, context);
                    configuration.database = checkAndGetLiteralArgument<String>(database_literal, "database");
                }
            }
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Unexpected key-value argument."
                        "Got: {}, but expected: sharding_key", arg_name);
        }
        cluster_description = configuration.addresses_expr;
        if (cluster_description.empty())
            cluster_description = configuration.port ? configuration.host + ':' + toString(configuration.port) : configuration.host;
    }
    else
    {
        if (args.size() < 2 || args.size() > max_args)
            throw Exception(help_message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        size_t arg_num = 0;
        auto get_string_literal = [](const IAST & node, String & res)
        {
            const auto * lit = node.as<ASTLiteral>();
            if (!lit)
                return false;

            if (lit->value.getType() != Field::Types::String)
                return false;

            res = lit->value.safeGet<const String &>();
            return true;
        };

        {
            if (!get_string_literal(*args[arg_num], cluster_description))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Hosts pattern must be string literal (in single quotes).");
        }

        ++arg_num;
        const auto * function = args[arg_num]->as<ASTFunction>();
        if (function && TableFunctionFactory::instance().isTableFunctionName(function->name))
        {
            remote_table_function_ptr = args[arg_num];
            ++arg_num;
        }
        else
        {
            args[arg_num] = evaluateConstantExpressionForDatabaseName(args[arg_num], context);
            configuration.database = checkAndGetLiteralArgument<String>(args[arg_num], "database");

            ++arg_num;

            auto qualified_name = QualifiedTableName::parseFromString(configuration.database);
            if (qualified_name.database.empty())
            {
                if (arg_num >= args.size())
                {
                    throw Exception(help_message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
                }
                else
                {
                    std::swap(qualified_name.database, qualified_name.table);
                    args[arg_num] = evaluateConstantExpressionOrIdentifierAsLiteral(args[arg_num], context);
                    qualified_name.table = checkAndGetLiteralArgument<String>(args[arg_num], "table");
                    ++arg_num;
                }
            }

            configuration.database = std::move(qualified_name.database);
            configuration.table = std::move(qualified_name.table);
        }

        /// Username and password parameters
        {
            if (arg_num < args.size())
            {
                if (!get_string_literal(*args[arg_num], configuration.username))
                {
                    configuration.username = "default";
                    sharding_key = args[arg_num];
                }
                ++arg_num;
            }

            if (arg_num < args.size() && !sharding_key)
            {
                if (!get_string_literal(*args[arg_num], configuration.password))
                {
                    sharding_key = args[arg_num];
                }
                ++arg_num;
            }

            if (arg_num < args.size() && !sharding_key)
            {
                sharding_key = args[arg_num];
                ++arg_num;
            }
        }

        if (arg_num < args.size())
            throw Exception(help_message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }

    {
        /// Create new cluster from the scratch
        size_t max_addresses = context->getSettingsRef().table_function_remote_max_addresses;
        std::vector<String> shards = parseRemoteDescription(cluster_description, 0, cluster_description.size(), ',', max_addresses);

        std::vector<std::vector<std::pair<String, cluster::NodeID>>> remote_addrs;
        remote_addrs.reserve(shards.size());
        for (const auto & shard : shards)
        {
            remote_addrs.emplace_back();
            auto & shard_addrs = remote_addrs.back();
            auto hosts = parseRemoteDescription(shard, 0, shard.size(), '|', max_addresses);
            for (auto & host : hosts)
                shard_addrs.emplace_back(std::move(host), cluster::Nulls::NullNodeID);
        }

        if (remote_addrs.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Shard list is empty after parsing first argument");

        /// Check host and port on affiliation allowed hosts.
        for (const auto & hosts : remote_addrs)
        {
            for (const auto & [host, _] : hosts)
            {
                size_t colon = host.find(':');
                if (colon == String::npos)
                    context->getRemoteHostFilter().checkHostAndPort(host, toString(secure ? DBMS_DEFAULT_SECURE_PORT : DBMS_DEFAULT_PORT));
                else
                    context->getRemoteHostFilter().checkHostAndPort(host.substr(0, colon), host.substr(colon + 1));
            }
        }

        bool treat_local_as_remote = false;
        bool treat_local_port_as_remote = context->getApplicationType() == Context::ApplicationType::LOCAL;
        cluster = std::make_shared<Cluster>(
            context->getSettingsRef(),
            remote_addrs,
            configuration.username,
            configuration.password,
            /*cluster_id=*/"",
            /*cluster_secret=*/"",
            context->getNodeID(),
            (secure ? DBMS_DEFAULT_SECURE_PORT : DBMS_DEFAULT_PORT),
            treat_local_as_remote,
            treat_local_port_as_remote,
            secure);
    }

    if (!remote_table_function_ptr && configuration.table.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The name of remote table cannot be empty");

    remote_table_id.database_name = configuration.database;
    remote_table_id.table_name = configuration.table;
}

StoragePtr TableFunctionRemote::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns) const
{
    /// StorageDistributed supports mismatching structure of remote table, so we can use outdated structure for CREATE ... AS remote(...)
    /// without additional conversion in StorageTableFunctionProxy
    if (cached_columns.empty())
        cached_columns = getActualTableStructure(context);

    assert(cluster);
    StoragePtr res = StorageDistributed::create(
        StorageID(getDatabaseName(), table_name),
        cached_columns,
        ConstraintsDescription{},
        String{},
        remote_table_id.database_name,
        remote_table_id.table_name,
        String{},
        context,
        sharding_key,
        String{},
        String{},
        DistributedSettings{},
        /*attach_=*/false,
        cluster,
        remote_table_function_ptr,
        /*is_remote_function_=*/true);

    res->startup();
    return res;
}

ColumnsDescription TableFunctionRemote::getActualTableStructure(ContextPtr context) const
{
    assert(cluster);
    return getStructureOfRemoteTable(*cluster, remote_table_id, context, remote_table_function_ptr);
}

TableFunctionRemote::TableFunctionRemote(const std::string & name_, bool secure_)
    : name{name_}, secure{secure_}
{
    help_message = PreformattedMessage::create(
        "Function '{}' requires from 2 to 6 parameters: "
        "<addresses pattern>, <name of remote database>, <name of remote table> [, username[, password], sharding_key]",
        name);
}

void registerTableFunctionRemote(TableFunctionFactory & factory)
{
    factory.registerFunction("remote", [] () -> TableFunctionPtr { return std::make_shared<TableFunctionRemote>("remote"); });
    factory.registerFunction("remote_secure", [] () -> TableFunctionPtr { return std::make_shared<TableFunctionRemote>("remote", /* secure = */ true); });
}

}
