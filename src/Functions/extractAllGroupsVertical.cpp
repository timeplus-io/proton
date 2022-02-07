#include <Functions/FunctionFactory.h>
#include <Functions/extractAllGroups.h>

namespace
{

struct VerticalImpl
{
    static constexpr auto Kind = DB::ExtractAllGroupsResultKind::VERTICAL;
    static constexpr auto Name = "extract_all_groups";
};

}

namespace DB
{

void registerFunctionExtractAllGroupsVertical(FunctionFactory & factory)
{
    factory.registerFunction<FunctionExtractAllGroups<VerticalImpl>>();
}

}
