#pragma once

#include "config.h"

namespace DB
{
class TableFunctionFactory;
void registerTableFunctionRemote(TableFunctionFactory & factory);
void registerTableFunctionNumbers(TableFunctionFactory & factory);
void registerTableFunctionNull(TableFunctionFactory & factory);
void registerTableFunctionZeros(TableFunctionFactory & factory);
void registerTableFunctionExecutable(TableFunctionFactory & factory);
void registerTableFunctionFile(TableFunctionFactory & factory);
void registerTableFunctionURL(TableFunctionFactory & factory);
void registerTableFunctionValues(TableFunctionFactory & factory);
void registerTableFunctionInput(TableFunctionFactory & factory);
void registerTableFunctionGenerate(TableFunctionFactory & factory);

#if USE_AWS_S3
void registerTableFunctionS3(TableFunctionFactory & factory);
void registerTableFunctionS3Cluster(TableFunctionFactory & factory);
void registerTableFunctionCOS(TableFunctionFactory & factory);
#endif

void registerTableFunctionView(TableFunctionFactory & factory);

void registerTableFunctionDictionary(TableFunctionFactory & factory);

void registerTableFunctionExplain(TableFunctionFactory & factory);

/// proton: starts
namespace Streaming
{
void registerTableFunctionHop(TableFunctionFactory & factory);
void registerTableFunctionTumble(TableFunctionFactory & factory);
void registerTableFunctionHist(TableFunctionFactory & factory);
void registerTableFunctionSession(TableFunctionFactory & factory);
void registerTableFunctionDedup(TableFunctionFactory & factory);
void registerTableFunctionChangelog(TableFunctionFactory & factory);
}
/// proton: ends

void registerTableFunctions();

}
