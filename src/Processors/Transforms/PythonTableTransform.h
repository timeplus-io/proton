#pragma once

#include "config.h"

#if USE_PYTHON_UDF

#include <CPython/PythonModuleSession.h>
#include <Processors/ISimpleTransform.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ExternalStream/Python/StoragePythonTable.h>

#include <mutex>

namespace DB
{
/// PythonTableTransform applies a Python external stream function per input chunk.
class PythonTableTransform final : public ISimpleTransform
{
public:
    PythonTableTransform(
        const Block & input_header,
        Block output_header,
        Names input_column_names_,
        ColumnsDescription output_columns_,
        Names requested_output_columns_,
        PythonTableMode mode_,
        cpython::PythonModuleSessionPtr session_);

    ~PythonTableTransform() override;

    String getName() const override { return "PythonTableTransform"; }

    Status prepare() override;
    void work() override;

protected:
    void onCancel() noexcept override;

    void transform(Chunk & chunk) override;

private:
    Block convertPythonResultToBlock(const cpython::PyObjectPtr & py_result) const;
    Block filterOutputBlock(Block && block) const;

    std::vector<size_t> input_positions;
    Names requested_output_columns;
    ColumnsDescription output_columns;
    DataTypePtr tuple_type;
    PythonTableMode mode;
    cpython::PythonModuleSessionPtr session;

    std::atomic<unsigned long> python_thread_id{0};
    std::atomic_bool cancel_requested{false};
    bool finalization_pending = false;
    bool finalization_completed = false;
    bool finalization_ignore_exceptions = true;
};
}

#endif
