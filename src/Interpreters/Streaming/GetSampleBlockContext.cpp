#include <Interpreters/Streaming/GetSampleBlockContext.h>

#include <Interpreters/SelectQueryOptions.h>

namespace DB
{
namespace Streaming
{
void GetSampleBlockContext::mergeSelectOptions(DB::SelectQueryOptions & select_options)
{
    select_options.setParentSelectHasAggregates(parent_select_has_aggregates);
    select_options.setParentSelectJoinStrictness(parent_select_join_strictness);
}
}
}