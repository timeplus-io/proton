#include <Processors/Sources/PythonStreamingSource.h>

#if USE_PYTHON_UDF

#include <CPython/ConvertDatatypes.h>
#include <CPython/GILGuard.h>
#include <CPython/Utils.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeTuple.h>
#include <Common/assert_cast.h>

namespace DB
{
namespace ErrorCodes
{
extern const int UDF_RUNNING_ERROR;
}

PythonStreamingSource::PythonStreamingSource(Block header, cpython::PyObjectPtr py_iterator_, DataTypePtr tuple_type_, String module_name_)
    : ISource(std::move(header), true, ProcessorID::PythonStreamingSourceID)
    , py_iterator(std::move(py_iterator_))
    , tuple_type(std::move(tuple_type_))
    , module_name(std::move(module_name_))
{
}

PythonStreamingSource::~PythonStreamingSource()
{
    if (py_iterator && Py_IsInitialized())
    {
        cpython::GILGuard gil_guard;
        py_iterator.reset();
        if (!module_name.empty())
            cpython::unloadModule(module_name);
    }
}

Block PythonStreamingSource::convertPythonResultToBlock(const cpython::PyObjectPtr & py_result) const
{
    Block res_block;

    const auto * tuple_type_ptr = assert_cast<const DataTypeTuple *>(tuple_type.get());
    auto normalized = cpython::normalizePythonListForTuple(py_result, tuple_type_ptr->getElements().size());
    auto result_column = cpython::convertPythonListToColumn(normalized, tuple_type);
    const auto & tuple_column = assert_cast<const ColumnTuple &>(*result_column);

    size_t elements = tuple_type_ptr->getElements().size();
    for (size_t i = 0; i < elements; ++i)
    {
        const auto & column = tuple_column.getColumnPtr(i);
        const auto & type = tuple_type_ptr->getElement(i);
        auto element_name = tuple_type_ptr->getNameByPosition(i + 1);
        res_block.insert(ColumnWithTypeAndName{column, type, element_name});
    }

    return res_block;
}

Chunk PythonStreamingSource::generate()
{
    if (exhausted)
        return {};

    if (Py_IsInitialized() == 0)
        throw Exception(ErrorCodes::UDF_RUNNING_ERROR, "Python Interpreter is not initialized, please check the python_path configuration");

    cpython::GILGuard gil_guard;

    auto next_item = cpython::iterNext(py_iterator);
    if (!next_item)
    {
        /// Iterator exhausted
        exhausted = true;
        return {};
    }

    /// Convert the yielded Python object to a Block
    /// The yielded item should be a list of tuples (batch of rows)
    auto block = convertPythonResultToBlock(next_item);

    if (block.rows() == 0)
        return {};

    return Chunk(block.getColumns(), block.rows());
}
}

#endif
