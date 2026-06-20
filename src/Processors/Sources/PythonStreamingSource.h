#pragma once

#include "config.h"

#if USE_PYTHON_UDF

#include <CPython/PythonModuleSession.h>
#include <Processors/ISource.h>


namespace DB
{
/// PythonStreamingSource reads data from a Python generator/iterator
/// and emits chunks incrementally as they are yielded.
class PythonStreamingSource final : public ISource
{
public:
    PythonStreamingSource(
        Block header, cpython::PyObjectPtr py_iterator_, DataTypePtr tuple_type_, cpython::PythonModuleSessionPtr session_);

    ~PythonStreamingSource() override;

    String getName() const override { return "PythonStreamingSource"; }

protected:
    void onCancel() noexcept override;

    Chunk generate() override;

private:
    Block convertPythonResultToBlock(const cpython::PyObjectPtr & py_result) const;

    Block convertPythonResultToOutputBlock(const cpython::PyObjectPtr & py_result) const;

    void init();
    void finishPython(bool ignore_exceptions, bool acquire_gil = true);

    cpython::PyObjectPtr py_iterator;
    DataTypePtr tuple_type;
    cpython::PythonModuleSessionPtr session;

    std::vector<size_t> output_tuple_positions;
    bool needs_projection_pushdown = false;

    bool exhausted = false;

    std::atomic<unsigned long> python_thread_id{0};
    std::atomic_bool cancel_requested{false};
    bool python_finished = false;
};
}

#endif
