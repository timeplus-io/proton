#include "registerTableFunctions.h"
#include <TableFunctions/TableFunctionFactory.h>

namespace DB
{
void registerTableFunctions()
{
    auto & factory = TableFunctionFactory::instance();

    registerTableFunctionRemote(factory);
    registerTableFunctionNumbers(factory);
    registerTableFunctionNull(factory);
    registerTableFunctionZeros(factory);
    registerTableFunctionExecutable(factory);
    registerTableFunctionFile(factory);
    registerTableFunctionURL(factory);
    registerTableFunctionValues(factory);
    registerTableFunctionInput(factory);
    registerTableFunctionGenerate(factory);

#if USE_AWS_S3
    registerTableFunctionS3(factory);
    registerTableFunctionS3Cluster(factory);
    registerTableFunctionCOS(factory);
#endif

    registerTableFunctionView(factory);

    registerTableFunctionDictionary(factory);

    registerTableFunctionExplain(factory);

    /// proton: starts
    Streaming::registerTableFunctionHop(factory);
    Streaming::registerTableFunctionTumble(factory);
    Streaming::registerTableFunctionHist(factory);
    Streaming::registerTableFunctionSession(factory);
    Streaming::registerTableFunctionDedup(factory);
    Streaming::registerTableFunctionChangelog(factory);
    /// proton: ends
}

}
