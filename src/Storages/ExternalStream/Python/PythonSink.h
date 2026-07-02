#pragma once

#include "config.h"

#if USE_PYTHON_UDF

#include <CPython/PythonModuleSession.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <base/types.h>

namespace DB
{
class PythonSink final : public SinkToStorage
{
public:
    PythonSink(const Block & header, cpython::PythonFunction function_);

    ~PythonSink() override;

    String getName() const override { return "PythonSink"; }

    void doCheckpoint(CheckpointContextPtr ckpt_ctx) override;

protected:
    void consume(Chunk chunk) override;
    void onFinish() override;

private:
    void finishPython(bool ignore_exceptions);

    cpython::PythonModuleSessionPtr session;
};
}

#endif
