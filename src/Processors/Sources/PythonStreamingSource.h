#pragma once

#include "config.h"

#if USE_PYTHON_UDF

#include <CPython/PyObjectPtr.h>
#include <Processors/ISource.h>


namespace DB
{
/// PythonStreamingSource reads data from a Python generator/iterator
/// and emits chunks incrementally as they are yielded.
class PythonStreamingSource final : public ISource
{
public:
    PythonStreamingSource(Block header, cpython::PyObjectPtr py_iterator_, DataTypePtr tuple_type_, String module_name_);

    ~PythonStreamingSource() override;

    String getName() const override { return "PythonStreamingSource"; }

protected:
    void onCancel() noexcept override;

    Chunk generate() override;

private:
    Block convertPythonResultToBlock(const cpython::PyObjectPtr & py_result) const;

    Block convertPythonResultToOutputBlock(const cpython::PyObjectPtr & py_result) const;

    cpython::PyObjectPtr py_iterator;
    DataTypePtr tuple_type;
    String module_name;

    std::vector<size_t> output_tuple_positions;
    bool needs_projection_pushdown = false;

    bool exhausted = false;

    std::atomic<unsigned long> python_thread_id{0};
    std::atomic_bool cancel_requested{false};
};
}

#endif
