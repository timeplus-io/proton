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
    const auto * tuple_type_ptr = assert_cast<const DataTypeTuple *>(tuple_type.get());
    const auto & output_header = getPort().getHeader();

    needs_projection_pushdown = output_header.columns() < tuple_type_ptr->getElements().size();
    output_tuple_positions.reserve(output_header.columns());

    for (const auto & output_col : output_header)
    {
        const auto pos = tuple_type_ptr->tryGetPositionByName(output_col.name);
        if (!pos)
            throw Exception(
                ErrorCodes::UDF_RUNNING_ERROR,
                "PythonStreamingSource header column '{}' not found in tuple type '{}'",
                output_col.name,
                tuple_type_ptr->getName());
        output_tuple_positions.emplace_back(*pos);
    }
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

Block PythonStreamingSource::convertPythonResultToOutputBlock(const cpython::PyObjectPtr & py_result) const
{
    Block res_block;

    const auto & output_header = getPort().getHeader();
    if (output_header.columns() == 0)
        return res_block;

    const auto * tuple_type_ptr = assert_cast<const DataTypeTuple *>(tuple_type.get());
    auto normalized = cpython::normalizePythonListForTuple(py_result, tuple_type_ptr->getElements().size());

    if (!normalized)
        return res_block;

    if (!PyList_Check(normalized.get()))
        throw Exception(
            ErrorCodes::UDF_RUNNING_ERROR,
            "PythonStreamingSource expected a list of rows from Python generator, got {}",
            cpython::getObjectType(normalized));

    const Py_ssize_t rows = PyList_Size(normalized.get());
    if (rows <= 0)
        return res_block;

    for (size_t out_idx = 0; out_idx < output_header.columns(); ++out_idx)
    {
        const auto tuple_pos = output_tuple_positions.at(out_idx);
        const auto & output_col = output_header.getByPosition(out_idx);

        cpython::PyObjectPtr values{PyList_New(rows)};
        if (!values)
            throw Exception(ErrorCodes::UDF_RUNNING_ERROR, "Failed to allocate Python list for projected column conversion");

        for (Py_ssize_t row_idx = 0; row_idx < rows; ++row_idx)
        {
            PyObject * row = PyList_GetItem(normalized.get(), row_idx);
            if (!row)
                throw Exception(ErrorCodes::UDF_RUNNING_ERROR, "Failed to access row {} from Python generator output", row_idx);

            PyObject * item = nullptr;
            if (PyTuple_Check(row))
                item = PyTuple_GetItem(row, tuple_pos);
            else if (PyList_Check(row))
                item = PyList_GetItem(row, tuple_pos);
            else
                throw Exception(
                    ErrorCodes::UDF_RUNNING_ERROR,
                    "PythonStreamingSource expected each row to be tuple/list, got {}",
                    cpython::getObjectType(cpython::PyObjectPtr::borrow(row)));

            if (!item)
            {
                if (cpython::hasException())
                    throw Exception(ErrorCodes::UDF_RUNNING_ERROR, "Failed to read projected value: {}", cpython::getExceptionMessage());
                throw Exception(ErrorCodes::UDF_RUNNING_ERROR, "Failed to read projected value (null)");
            }

            Py_INCREF(item);
            PyList_SET_ITEM(values.get(), row_idx, item);
        }

        auto column = cpython::convertPythonListToColumn(values, output_col.type);
        res_block.insert(ColumnWithTypeAndName{std::move(column), output_col.type, output_col.name});
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
    auto block = needs_projection_pushdown ? convertPythonResultToOutputBlock(next_item) : convertPythonResultToBlock(next_item);

    if (block.rows() == 0)
        return {};

    /// StoragePythonTable may request only a subset of columns (projection pushdown).
    /// Python generator still yields full rows, so we must align the emitted chunk with OutputPort header.
    const auto & output_header = getPort().getHeader();
    Columns output_columns;
    output_columns.reserve(output_header.columns());
    for (const auto & header_col : output_header)
        output_columns.emplace_back(block.getByName(header_col.name).column);

    return Chunk(std::move(output_columns), block.rows());
}
}

#endif
