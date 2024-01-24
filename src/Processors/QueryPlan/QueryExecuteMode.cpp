#include "QueryExecuteMode.h"

#include <Core/Settings.h>

namespace DB
{
namespace ErrorCodes
{
extern const int INVALID_SETTING_VALUE;
extern const int NOT_IMPLEMENTED;
}

ExecuteMode queryExecuteMode(bool is_streaming, bool is_subquery, const Settings & settings)
{
    switch (settings.exec_mode)
    {
        case ExecuteMode::NORMAL:
            break;
        case ExecuteMode::SUBSCRIBE:
        case ExecuteMode::UNSUBSCRIBE:
        case ExecuteMode::RECOVER:
            if (!is_streaming)
            {
                /// Assume it's a historical subquery in streaming query, we only execute it in normal mode.
                if (is_subquery)
                    return ExecuteMode::NORMAL;

                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "SUBSCRIBE can only work with streaming query");
            }
            break;
    }

    return settings.exec_mode;
}
}
