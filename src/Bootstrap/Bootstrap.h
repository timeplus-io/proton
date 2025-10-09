#pragma once

#include <Interpreters/Context_fwd.h>

namespace Poco
{
class Logger;
}

namespace DB
{
void bootstrap(ContextMutablePtr global_context, Poco::Logger * logger);

/// We need make sure teardown bootstrapped global instances
/// first before we tear down the proton instance
void teardown(LoggerPtr logger);
}
