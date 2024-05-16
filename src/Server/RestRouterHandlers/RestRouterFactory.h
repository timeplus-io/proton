#pragma once

#include "APISpecHandler.h"
#include "ClusterInfoHandler.h"
#include "ColumnRestRouterHandler.h"
#include "DatabaseRestRouterHandler.h"
#include "ExternalStreamRestRouterHandler.h"
#include "IngestRawStoreHandler.h"
#include "IngestRestRouterHandler.h"
#include "IngestStatusHandler.h"
#include "MetaStoreHandler.h"
#include "PingHandler.h"
#include "PipelineMetricHandler.h"
#include "RawstoreTableRestRouterHandler.h"
#include "RestRouterHandler.h"
#include "SQLAnalyzerRestRouterHandler.h"
#include "SQLFormatHandler.h"
#include "SearchHandler.h"
#include "StorageInfoHandler.h"
#include "SystemCommandHandler.h"
#include "TabularTableRestRouterHandler.h"
#include "UDFHandler.h"

#include <re2/re2.h>
#include <Common/escapeForFileName.h>

#include <unordered_map>

namespace DB
{
using CompiledRegexPtr = std::shared_ptr<const re2::RE2>;

namespace ErrorCodes
{
extern const int CANNOT_COMPILE_REGEXP;
}

class RestRouterFactory final
{
public:
    static RestRouterFactory & instance()
    {
        static RestRouterFactory router_factory;
        return router_factory;
    }

    static void registerRestRouterHandlers()
    {
        constexpr auto * stream_name_regex = "(?P<stream>[_%\\.\\-\\w]+)";

        auto & factory = RestRouterFactory::instance();
        for (const auto * prefix : {"proton", "timeplusd"})
        {
            factory.registerRouterHandler(
                fmt::format("/{}/v1/ingest/streams/{}(\\?mode=\\w+){{0,1}}", prefix, stream_name_regex),
                "POST",
                [](ContextMutablePtr query_context) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
                    return std::make_shared<IngestRestRouterHandler>(query_context);
                });

            factory.registerRouterHandler(
                fmt::format("/{}/v1/ingest/rawstores/{}(\\?mode=\\w+){{0,1}}", prefix, stream_name_regex),
                "POST",
                [](ContextMutablePtr query_context) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
                    return std::make_shared<IngestRawStoreHandler>(query_context);
                });

            factory.registerRouterHandler(
                fmt::format("/{}/v1/search(\\?[\\w\\-=&#]+){{0,1}}", prefix),
                "POST",
                [](ContextMutablePtr query_context) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
                    return std::make_shared<DB::SearchHandler>(query_context);
                });

            factory.registerRouterHandler(
                fmt::format("/{}/v1/pipeline_metrics", prefix),
                "POST",
                [](ContextMutablePtr query_context) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
                    return std::make_shared<DB::PipelineMetricHandler>(query_context);
                });

            factory.registerRouterHandler(
                fmt::format("/{}/v1/ingest/statuses", prefix),
                "POST",
                [](ContextMutablePtr query_context) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
                    return std::make_shared<IngestStatusHandler>(query_context);
                });

            factory.registerRouterHandler(
                /// PATH: '/proton/v1/ddl/streams [/{databse}] [/{key}] ...'
                fmt::format(
                    "/{}/v1/ddl/streams(/?(\\?[\\w\\-=&#]+){{0,1}}$|/(?P<database>[%\\w]+)(/?(\\?[\\w\\-=&#]+){{0,1}}$|/"
                    "(?P<stream>[%\\-\\.\\w]+)(\\?[\\w\\-=&#]+){{0,1}})(\\?[\\w\\-=&#]+){{0,1}})",
                    prefix),
                "GET",
                [](ContextMutablePtr query_context) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
                    return std::make_shared<TabularTableRestRouterHandler>(query_context);
                });

            factory.registerRouterHandler(
                fmt::format("/{}/v1/ddl/streams(?:\\?\?[^/]*)?", prefix),
                "POST",
                [](ContextMutablePtr query_context) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
                    return std::make_shared<TabularTableRestRouterHandler>(query_context);
                });

            factory.registerRouterHandler(
                fmt::format("/{}/v1/ddl/streams/{}(\\?[\\w\\-=&#]+){{0,1}}", prefix, stream_name_regex),
                "PATCH/DELETE",
                [](ContextMutablePtr query_context) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
                    return std::make_shared<TabularTableRestRouterHandler>(query_context);
                });

            factory.registerRouterHandler(
                fmt::format("/{}/v1/ddl/externalstreams(\\?[\\w\\-=&#]+){{0,1}}", prefix),
                "POST",
                [](ContextMutablePtr query_context) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
                    return std::make_shared<ExternalStreamRestRouterHandler>(query_context);
                });

            factory.registerRouterHandler(
                fmt::format("/{}/v1/ddl/rawstores(\\?[\\w\\-=&#]+){{0,1}}", prefix),
                "GET/POST",
                [](ContextMutablePtr query_context) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
                    return std::make_shared<RawstoreTableRestRouterHandler>(query_context);
                });

            factory.registerRouterHandler(
                fmt::format("/{}/v1/ddl/rawstores/{}(\\?[\\w\\-=&#]+){{0,1}}", prefix, stream_name_regex),
                "DELETE" /* So far, not support PATCH */,
                [](ContextMutablePtr query_context) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
                    return std::make_shared<RawstoreTableRestRouterHandler>(query_context);
                });

            /// So far, we do not support ALTER TABLE ADD/UPDATE/DELETE COLUMN ...
            // factory.registerRouterHandler(
            //     fmt::format("/proton/v1/ddl/{}/columns(\\?[\\w\\-=&#]+){{0,1}}", stream_name_regex),
            //     "GET/POST",
            //     [](ContextMutablePtr query_context) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
            //         return std::make_shared<ColumnRestRouterHandler>(query_context);
            //     });

            // factory.registerRouterHandler(
            //     fmt::format("/proton/v1/ddl/{}/columns/(?P<column>[%\\w]+)(\\?[\\w\\-=&#]+){{0,1}}", stream_name_regex},
            //     "PATCH/DELETE",
            //     [](ContextMutablePtr query_context) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
            //         return std::make_shared<ColumnRestRouterHandler>(query_context);
            //     });

            factory.registerRouterHandler(
                fmt::format("/{}/v1/ddl/databases(\\?[\\w\\-=&#]+){{0,1}}", prefix),
                "GET/POST",
                [](ContextMutablePtr query_context) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
                    return std::make_shared<DB::DatabaseRestRouterHandler>(query_context);
                });

            factory.registerRouterHandler(
                fmt::format("/{}/v1/ddl/databases/(?P<database>\\w+)(\\?[\\w\\-=&#]+){{0,1}}", prefix),
                "DELETE",
                [](ContextMutablePtr query_context) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
                    return std::make_shared<DB::DatabaseRestRouterHandler>(query_context);
                });

            factory.registerRouterHandler(
                fmt::format("/{}/v1/sqlanalyzer", prefix),
                "POST",
                [](ContextMutablePtr query_context) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
                    return std::make_shared<SQLAnalyzerRestRouterHandler>(query_context);
                });

            factory.registerRouterHandler(
                fmt::format("/{}/v1/sqlformat", prefix),
                "POST",
                [](ContextMutablePtr query_context) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
                    return std::make_shared<SQLFormatHandler>(query_context);
                });

            /// GET/POST/DELETE: /proton/v1/udfs[/{func}]
            factory.registerRouterHandler(
                fmt::format("/{}/v1/udfs(/?$|/(?P<func>[%\\w]+))", prefix),
                "GET/POST/DELETE",
                [](ContextMutablePtr query_context) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
                    return std::make_shared<UDFHandler>(query_context);
                });

            factory.registerRouterHandler(
                fmt::format("/{}/(?P<status>ping|info)$", prefix),
                "GET",
                [](ContextMutablePtr query_context) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
                    return std::make_shared<DB::PingHandler>(query_context);
                });

            factory.registerRouterHandler(
                fmt::format("/{}/v1/clusterinfo", prefix),
                "GET",
                [](ContextMutablePtr query_context) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
                    return std::make_shared<DB::ClusterInfoHandler>(query_context);
                });

            /// GET: /proton/v1/storageinfo[/{database}[/{stream}]]
            factory.registerRouterHandler(
                fmt::format(
                    "/{}/v1/storageinfo(/?(\\?[\\w\\-=&#]+){{0,1}}$|/(?P<database>[%\\w]+)(/?(\\?[\\w\\-=&#]+){{0,1}}$|/"
                    "(?P<stream>[%\\-\\.\\w]+)(\\?[\\w\\-=&#]+){{0,1}})(\\?[\\w\\-=&#]+){{0,1}})",
                    prefix),
                "GET",
                [](ContextMutablePtr query_context) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
                    return std::make_shared<DB::StorageInfoHandler>(query_context);
                });

            /// POST /proton/v1/system?<params>
            factory.registerRouterHandler(
                fmt::format("/{}/v1/system(\\?[\\w\\-=&#]+){{0,1}}", prefix),
                "POST",
                [](ContextMutablePtr query_context) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
                    return std::make_shared<DB::SystemCommandHandler>(query_context);
                });

            factory.registerRouterHandler(
                fmt::format("/{}/apis", prefix),
                "GET",
                [](ContextMutablePtr query_context) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
                    return std::make_shared<DB::APISpecHandler>(query_context);
                });
        }
    }

    static void registerMetaStoreRestRouterHandlers()
    {
        auto & factory = RestRouterFactory::instance();
#if USE_NURAFT
        /// GET/POST/DELETE: /proton/metastore/{namespace} [/{key}] [?{params}]
        factory.registerRouterHandler(
            "/proton/metastore/[\\w\\W]+",
            "GET/POST/DELETE",
            [](ContextMutablePtr query_context) { /// STYLE_CHECK_ALLOW_BRACE_SAME_LINE_LAMBDA
                return std::make_shared<DB::MetaStoreHandler>(query_context);
            });
#endif
    }

public:
    RestRouterHandlerPtr get(const String & url, const String & method, ContextMutablePtr query_context) const
    {
        for (auto & router_handler : router_handlers)
        {
            int num_captures = router_handler.regex->NumberOfCapturingGroups() + 1;

            /// Match request method
            if (router_handler.method.find(method) != String::npos)
            {
                /// Captures param value
                re2::StringPiece matches[num_captures];

                /// Match request url
                if (router_handler.regex->Match(url, 0, url.size(), re2::RE2::Anchor::ANCHOR_BOTH, matches, num_captures))
                {
                    auto handler = router_handler.handler(query_context);

                    /// Get param name
                    for (const auto & [capturing_name, capturing_index] : router_handler.regex->NamedCapturingGroups())
                    {
                        const auto & capturing_value = matches[capturing_index];
                        if (capturing_value.data())
                        {
                            /// Put path parameters into handler map<string name, string value>
                            const auto & parameter_value = unescapeForFileName(String(capturing_value.data(), capturing_value.size()));
                            handler->setPathParameter(capturing_name, parameter_value);
                        }
                    }

                    return handler;
                }
            }
        }

        return nullptr;
    }

    void registerRouterHandler(const String & route, const String & method, std::function<RestRouterHandlerPtr(ContextMutablePtr)> func)
    {
        auto regex = compileRegex(route);
        router_handlers.emplace_back(RouterHandler(method, regex, func));
    }

private:
    CompiledRegexPtr compileRegex(const String & expression)
    {
        auto compiled_regex = std::make_shared<const re2::RE2>(expression);

        if (!compiled_regex->ok())
        {
            throw Exception(
                ErrorCodes::CANNOT_COMPILE_REGEXP,
                "Cannot compile re2: '{}' for http handling rule, error: {}. Look at https://github.com/google/re2/wiki/Syntax for "
                "reference.",
                expression,
                compiled_regex->error());
        }

        return compiled_regex;
    }

    struct RouterHandler
    {
        String method;
        CompiledRegexPtr regex;
        std::function<RestRouterHandlerPtr(ContextMutablePtr)> handler;

        RouterHandler(
            const String & method_, const CompiledRegexPtr & regex_, std::function<RestRouterHandlerPtr(ContextMutablePtr)> handler_)
            : method(method_), regex(regex_), handler(handler_)
        {
        }
    };

    std::vector<RouterHandler> router_handlers;
};

}
