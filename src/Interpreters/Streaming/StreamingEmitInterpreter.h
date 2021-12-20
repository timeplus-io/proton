#pragma once

#include <Parsers/IAST_fwd.h>
#include <boost/noncopyable.hpp>
#include <Core/Settings.h>
#include <Poco/Logger.h>

/// For streaming query, there are some extension grammar rules to provides simpler user experiences as follows:
/// Rule-1 <LastX>
/// -------------
/// EMIT STREAM [LAST <last-x>] ...
///
/// ...
/// Note: The `Rule` can be any executable object(function/pseudo-function/lambada)
/// with a required parameter —— (ASTPtr & query)
namespace DB
{
class ASTSelectQuery;

class StreamingEmitInterpreter final
{
public:
    template <typename... Rules>
    static void handleRules(ASTPtr & query, Rules &&... rules)
    {
        (rules(query), ...);
    }

public:
    /// [Rule] Last X
    /// -------------
    /// EMIT STREAM [LAST <last-x>]
    /// <last-x> is Interval alias, such as : 1s 1m 1h
    ///
    /// Note: Last X streaming processing is just based on existing streaming processing primitives
    /// but provides simpler user experiences.
    class LastXRule final
    {
    private:
        const Settings & settings;
        Poco::Logger * log;
        ASTPtr query;
        ASTPtr emit_query;
        ASTPtr last_interval;

    public:
        LastXRule(const Settings & settings_, Poco::Logger * log_ = nullptr);
        void operator()(ASTPtr & query);

    private:
        /// Last X streaming processing for window(Tumble/Hop...)
        /// we shall convert last_interval to settings "keep_windows = `ceil(last_interval / window_interval)`" for AST
        bool handleWindowAggr(ASTSelectQuery & query);
        /// Last X streaming processing for gloabl aggregation
        /// we shall convert gloabl aggreation to hop table window for AST
        bool handleGlobalAggr(ASTSelectQuery & query);
    };
};
}
