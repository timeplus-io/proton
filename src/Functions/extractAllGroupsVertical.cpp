#include <Functions/FunctionFactory.h>
#include <Functions/extractAllGroups.h>

namespace
{

struct VerticalImpl
{
    static constexpr auto Kind = DB::ExtractAllGroupsResultKind::VERTICAL;
    static constexpr auto Name = "extract_all_groups_vertical";
};

}

namespace DB
{

REGISTER_FUNCTION(ExtractAllGroupsVertical)
{
    factory.registerFunction<FunctionExtractAllGroups<VerticalImpl>>();
    factory.registerAlias("extract_all_groups", VerticalImpl::Name, FunctionFactory::CaseSensitive);
}

}
