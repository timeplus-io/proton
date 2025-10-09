#include <Common/Exception.h>

#include "config.h"

#if USE_PYTHON_UDF
#include <Python.h>

namespace DB
{

namespace ErrorCodes
{
extern const int RESOURCE_NOT_INITED;
}

inline void ensurePythonInited()
{
    if (!Py_IsInitialized())
        throw Exception(ErrorCodes::RESOURCE_NOT_INITED, "Embedded Python interpreter is not initialized or the initialization failed");
}

}
#else

namespace DB
{

inline void ensurePythonInited()
{
}

}
#endif
