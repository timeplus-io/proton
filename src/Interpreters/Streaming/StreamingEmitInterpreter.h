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
    public:
        LastXRule(const Settings & settings_, Int64 & last_interval_seconds_, bool & tail_, Poco::Logger * log_ = nullptr);
        void operator()(ASTPtr & query);

        bool isTail() const { return tail; }
        Int64 lastIntervalSeconds() const { return last_interval_seconds; }

    private:
        /// Last X streaming processing for window(Tumble/Hop...)
        /// we shall convert last_interval to settings "keep_windows = `ceil(last_interval / window_interval)`" for AST
        bool handleWindowAggr(ASTSelectQuery & query);

        /// Last X streaming processing for global aggregation
        /// we shall convert global aggregation to hop table window for AST
        bool handleGlobalAggr(ASTSelectQuery & query);

        /// Last X streaming tail
        void handleTail(ASTSelectQuery & query);

    private:
        const Settings & settings;
        Int64 & last_interval_seconds;
        bool & tail;
        Poco::Logger * log;

        ASTPtr query;
        ASTPtr emit_query;
        ASTPtr last_interval;
    };
};
}
