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
class BaseScaleInterval;

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
        LastXRule(const Settings & settings_, BaseScaleInterval & last_interval_bs_, bool & tail_, Poco::Logger * log_ = nullptr);
        void operator()(ASTPtr & query);

        bool isTail() const { return tail; }
        const BaseScaleInterval & lastInterval() const { return last_interval_bs; }

    private:
        /// Last X streaming processing for window(Tumble/Hop...)
        /// we shall convert last_interval to settings "keep_windows = `ceil(last_interval / window_interval)`" for AST
        bool handleWindowAggr(ASTSelectQuery & query) const;

        /// Last X streaming processing for global aggregation
        /// we shall convert global aggregation to hop table window for AST
        bool handleGlobalAggr(ASTSelectQuery & query);

        /// Last X streaming tail
        void handleTail(ASTSelectQuery & query) const;

        void addEventTimePredicate(ASTSelectQuery & query) const;

    private:
        const Settings & settings;
        BaseScaleInterval & last_interval_bs;
        bool & tail;
        Poco::Logger * log;

        ASTPtr query;
        ASTPtr emit_query;
        ASTPtr last_interval;
        bool proc_time = false;
    };

    /// To check emit ast
    static void checkEmitAST(ASTPtr & query);
};
}
