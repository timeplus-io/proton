#include <config_core.h>

namespace DB
{
class FunctionFactory;

#if USE_MATH_FUNCS
void registerFunctionE(FunctionFactory & factory);
void registerFunctionPi(FunctionFactory & factory);
void registerFunctionExp(FunctionFactory & factory);
void registerFunctionLog(FunctionFactory & factory);
void registerFunctionExp2(FunctionFactory & factory);
void registerFunctionLog2(FunctionFactory & factory);
void registerFunctionLog1p(FunctionFactory & factory);
void registerFunctionExp10(FunctionFactory & factory);
void registerFunctionLog10(FunctionFactory & factory);
void registerFunctionSqrt(FunctionFactory & factory);
void registerFunctionCbrt(FunctionFactory & factory);
void registerFunctionErf(FunctionFactory & factory);
void registerFunctionErfc(FunctionFactory & factory);
void registerFunctionLGamma(FunctionFactory & factory);
void registerFunctionTGamma(FunctionFactory & factory);
void registerFunctionSin(FunctionFactory & factory);
void registerFunctionCos(FunctionFactory & factory);
void registerFunctionTan(FunctionFactory & factory);
void registerFunctionAsin(FunctionFactory & factory);
void registerFunctionAcos(FunctionFactory & factory);
void registerFunctionAtan(FunctionFactory & factory);
void registerFunctionAtan2(FunctionFactory & factory);
void registerFunctionSigmoid(FunctionFactory & factory);
void registerFunctionHypot(FunctionFactory & factory);
void registerFunctionSinh(FunctionFactory & factory);
void registerFunctionCosh(FunctionFactory & factory);
void registerFunctionTanh(FunctionFactory & factory);
void registerFunctionAsinh(FunctionFactory & factory);
void registerFunctionAcosh(FunctionFactory & factory);
void registerFunctionAtanh(FunctionFactory & factory);
void registerVectorFunctions(FunctionFactory &);
void registerFunctionDegrees(FunctionFactory & factory);
void registerFunctionRadians(FunctionFactory & factory);
#endif

void registerFunctionPow(FunctionFactory & factory);
void registerFunctionSign(FunctionFactory & factory);
void registerFunctionMax2(FunctionFactory & factory);
void registerFunctionMin2(FunctionFactory & factory);

void registerFunctionsMath(FunctionFactory & factory)
{
#if USE_MATH_FUNCS
    registerFunctionE(factory);
    registerFunctionPi(factory);
    registerFunctionExp(factory);
    registerFunctionLog(factory);
    registerFunctionExp2(factory);
    registerFunctionLog2(factory);
    registerFunctionLog1p(factory);
    registerFunctionExp10(factory);
    registerFunctionLog10(factory);
    registerFunctionSqrt(factory);
    registerFunctionCbrt(factory);
    registerFunctionErf(factory);
    registerFunctionErfc(factory);
    registerFunctionLGamma(factory);
    registerFunctionTGamma(factory);
    registerFunctionSin(factory);
    registerFunctionCos(factory);
    registerFunctionTan(factory);
    registerFunctionAsin(factory);
    registerFunctionAcos(factory);
    registerFunctionAtan(factory);
    registerFunctionAtan2(factory);
    registerFunctionSigmoid(factory);
    registerFunctionHypot(factory);
    registerFunctionSinh(factory);
    registerFunctionCosh(factory);
    registerFunctionTanh(factory);
    registerFunctionAsinh(factory);
    registerFunctionAcosh(factory);
    registerFunctionAtanh(factory);
    registerVectorFunctions(factory);
    registerFunctionDegrees(factory);
    registerFunctionRadians(factory);
#endif
    registerFunctionPow(factory);
    registerFunctionSign(factory);
    registerFunctionMax2(factory);
    registerFunctionMin2(factory);
}

}
