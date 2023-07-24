#pragma once

#include <Interpreters/DatabaseAndTableWithAlias.h>
#include <Interpreters/IdentifierSemantic.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Storages/Streaming/SeekToInfo.h>

namespace DB
{
class ASTFunction;

namespace Streaming
{
enum class SeekBy : uint8_t
{
    None,
    EventTime,
    EventSequenceNumber
};


class EventPredicateMatcher
{
public:
    using Visitor = InDepthNodeVisitor<EventPredicateMatcher, true, false>;

    struct Data : WithContext
    {
    private:
        /// Allow multiple streams: stream_pos - SeekToInfos
        SeekToInfosOfStreams seek_to_infos;
        /// FIXME, the init of IdentifierMembershipCollector could be expensive since it needs walk
        /// through the AST to resolve the table expression which will recursively call
        /// InterpreterSelectWithUnionQuery::getSampleBlock(subquery, ...) to evaluate the sample header
        /// if the table expression is a subquery (this is mostly true for join case)
        /// Check the sample block cache hit rate
        const IdentifierMembershipCollector membership_collector;

    public:
        Data(const ASTSelectQuery & select, ContextPtr context_) : WithContext(context_), membership_collector(select, getContext()) { }

        SeekToInfoPtr tryGetSeekToInfoForLeftStream() const { return tryGetSeekToInfo(0); }
        SeekToInfoPtr tryGetSeekToInfoForRightStream() const { return tryGetSeekToInfo(1); }

    private:
        SeekToInfoPtr tryGetSeekToInfo(size_t stream_pos) const;
        std::pair<size_t, SeekToInfoPtr> parseSeekToInfo(const ASTFunction & func, ASTPtr & ast) const;
        std::tuple<size_t, SeekBy, Int64, bool> parseEventPredicate(ASTPtr left_ast, ASTPtr right_ast) const;
        std::pair<size_t, SeekBy> parseSeekBy(ASTPtr ast) const;

        friend class EventPredicateMatcher;
    };

    static void visit(ASTPtr & ast, Data & data);
    static bool needChildVisit(ASTPtr & node, ASTPtr & child);
};

/// Visit `WHERE` clause to parse event predicate like `WHERE _tp_time > ...` to generate the corresponding
/// `SeekToInfo` for streaming store.
using EventPredicateVisitor = EventPredicateMatcher::Visitor;
}
}
