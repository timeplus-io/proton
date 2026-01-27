#pragma once

#include "config.h"

#if USE_PYTHON_UDF

#include <CPython/PyObjectPtr.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <base/types.h>

namespace DB
{
class PythonSink final : public SinkToStorage
{
public:
    PythonSink(const Block & header, String python_source_, String function_name_);

    ~PythonSink() override;

    String getName() const override { return "PythonSink"; }

protected:
    void consume(Chunk chunk) override;

private:
    void initPython();

    String python_source;
    String function_name;
    String module_name;
    cpython::PyObjectPtr py_function;
};
}

#endif
