#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsRound.h>


namespace DB
{

void registerFunctionsRound(FunctionFactory & factory)
{
    factory.registerFunction<FunctionRound>("round", FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionRoundBankers>("round_bankers", FunctionFactory::CaseSensitive);
    factory.registerFunction<FunctionFloor>("floor", FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionCeil>("ceil", FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionTrunc>("trunc", FunctionFactory::CaseInsensitive);
    factory.registerFunction<FunctionRoundDown>();
}

}
