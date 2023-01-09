#pragma once

#include <Interpreters/InDepthNodeVisitor.h>
#include <Storages/Streaming/SeekToInfo.h>

namespace DB
{
class ASTFunction;

class EventPredicateMatcher
{
public:
    using Visitor = InDepthNodeVisitor<EventPredicateMatcher, true, false>;

    struct Data
    {
        ContextPtr context;
        SeekToInfoPtr seek_to_info;
        ASTPtr event_predicate;

        Data(ContextPtr context_) : context(std::move(context_)) {}
    };

    static void visit(ASTPtr & ast, Data & data);
    static bool needChildVisit(ASTPtr & node, ASTPtr & child);
};

/// Visit `WHERE` clause to parse event predicate like `WHERE _tp_time > ...` to generate the corresponding
/// `SeekToInfo` for streaming store.
using EventPredicateVisitor = EventPredicateMatcher::Visitor;
}
