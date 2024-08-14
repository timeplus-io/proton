#include <Common/Exception.h>
#include <Common/FieldVisitorToString.h>

#include <Core/Block.h>

#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

#include <Common/assert_cast.h>

#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnSparse.h>

#include <iterator>
#include <base/sort.h>
#include <boost/algorithm/string.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int POSITION_OUT_OF_BOUND;
    extern const int NOT_FOUND_COLUMN_IN_BLOCK;
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
    extern const int AMBIGUOUS_COLUMN_NAME;
}

template <typename ReturnType>
static ReturnType onError(const std::string & message [[maybe_unused]], int code [[maybe_unused]])
{
    if constexpr (std::is_same_v<ReturnType, void>)
        throw Exception(message, code);
    else
        return false;
}


template <typename ReturnType>
static ReturnType checkColumnStructure(const ColumnWithTypeAndName & actual, const ColumnWithTypeAndName & expected,
    std::string_view context_description, bool allow_materialize, bool ignore_name, int code)
{
    if (!ignore_name && actual.name != expected.name)
        return onError<ReturnType>("Block structure mismatch in " + std::string(context_description) + " stream: different names of columns:\n"
            + actual.dumpStructure() + "\n" + expected.dumpStructure(), code);

    if ((actual.type && !expected.type) || (!actual.type && expected.type)
        || (actual.type && expected.type && !actual.type->equals(*expected.type)))
        return onError<ReturnType>("Block structure mismatch in " + std::string(context_description) + " stream: different types:\n"
            + actual.dumpStructure() + "\n" + expected.dumpStructure(), code);

    if (!actual.column || !expected.column)
        return ReturnType(true);

    const IColumn * actual_column = actual.column.get();

    /// If we allow to materialize, and expected column is not const or sparse, then unwrap actual column.
    if (allow_materialize)
    {
        if (!isColumnConst(*expected.column))
            if (const auto * column_const = typeid_cast<const ColumnConst *>(actual_column))
                actual_column = &column_const->getDataColumn();

        if (!expected.column->isSparse())
            if (const auto * column_sparse = typeid_cast<const ColumnSparse *>(actual_column))
                actual_column = &column_sparse->getValuesColumn();
    }

    const auto * actual_column_maybe_agg = typeid_cast<const ColumnAggregateFunction *>(actual_column);
    const auto * expected_column_maybe_agg = typeid_cast<const ColumnAggregateFunction *>(expected.column.get());
    if (actual_column_maybe_agg && expected_column_maybe_agg)
    {
        if (!actual_column_maybe_agg->getAggregateFunction()->haveSameStateRepresentation(*expected_column_maybe_agg->getAggregateFunction()))
            return onError<ReturnType>(
                fmt::format(
                    "Block structure mismatch in {} stream: different columns:\n{}\n{}",
                    context_description,
                    actual.dumpStructure(),
                    expected.dumpStructure()),
                code);
    }
    else if (actual_column->getName() != expected.column->getName())
        return onError<ReturnType>(
            fmt::format(
                "Block structure mismatch in {} stream: different columns:\n{}\n{}",
                context_description,
                actual.dumpStructure(),
                expected.dumpStructure()),
            code);

    if (isColumnConst(*actual.column) && isColumnConst(*expected.column))
    {
        Field actual_value = assert_cast<const ColumnConst &>(*actual.column).getField();
        Field expected_value = assert_cast<const ColumnConst &>(*expected.column).getField();

        if (actual_value != expected_value)
            return onError<ReturnType>("Block structure mismatch in " + std::string(context_description) + " stream: different values of constants, actual: "
                + applyVisitor(FieldVisitorToString(), actual_value) + ", expected: " + applyVisitor(FieldVisitorToString(), expected_value),
                code);
    }

    return ReturnType(true);
}


template <typename ReturnType>
static ReturnType checkBlockStructure(const Block & lhs, const Block & rhs, std::string_view context_description, bool allow_materialize, bool ignore_name)
{
    size_t columns = rhs.columns();
    if (lhs.columns() != columns)
        return onError<ReturnType>("Block structure mismatch in " + std::string(context_description) + " stream: different number of columns:\n"
            + lhs.dumpStructure() + "\n" + rhs.dumpStructure(), ErrorCodes::LOGICAL_ERROR);

    for (size_t i = 0; i < columns; ++i)
    {
        const auto & actual = lhs.getByPosition(i);
        const auto & expected = rhs.getByPosition(i);

        if constexpr (std::is_same_v<ReturnType, bool>)
        {
            if (!checkColumnStructure<ReturnType>(actual, expected, context_description, allow_materialize, ignore_name, ErrorCodes::LOGICAL_ERROR))
                return false;
        }
        else
            checkColumnStructure<ReturnType>(actual, expected, context_description, allow_materialize, ignore_name, ErrorCodes::LOGICAL_ERROR);
    }

    return ReturnType(true);
}


Block::Block(std::initializer_list<ColumnWithTypeAndName> il) : data{il}
{
    initializeIndexByName();
}


Block::Block(const ColumnsWithTypeAndName & data_) : data{data_}
{
    initializeIndexByName();
}

Block::Block(ColumnsWithTypeAndName && data_) : data{std::move(data_)}
{
    initializeIndexByName();
}

void Block::initializeIndexByName()
{
    for (size_t i = 0, size = data.size(); i < size; ++i)
        index_by_name.emplace(data[i].name, i);
}

void Block::insert(size_t position, ColumnWithTypeAndName elem)
{
    if (position > data.size())
        throw Exception("Position out of bound in Block::insert(), max position = "
        + toString(data.size()), ErrorCodes::POSITION_OUT_OF_BOUND);

    if (elem.name.empty())
        throw Exception("Column name in Block cannot be empty", ErrorCodes::AMBIGUOUS_COLUMN_NAME);

    auto [new_it, inserted] = index_by_name.emplace(elem.name, position);
    if (!inserted)
        checkColumnStructure<void>(data[new_it->second], elem,
            "(columns with identical name must have identical structure)", true, false, ErrorCodes::AMBIGUOUS_COLUMN_NAME);

    for (auto it = index_by_name.begin(); it != index_by_name.end(); ++it)
    {
        if (it->second >= position && (!inserted || it != new_it))
            ++it->second;
    }

    data.emplace(data.begin() + position, std::move(elem));
}


void Block::insert(ColumnWithTypeAndName elem)
{
    if (elem.name.empty())
        throw Exception("Column name in Block cannot be empty", ErrorCodes::AMBIGUOUS_COLUMN_NAME);

    auto [it, inserted] = index_by_name.emplace(elem.name, data.size());
    if (!inserted)
        checkColumnStructure<void>(data[it->second], elem,
            "(columns with identical name must have identical structure)", true, false, ErrorCodes::AMBIGUOUS_COLUMN_NAME);

    data.emplace_back(std::move(elem));
}


void Block::insertUnique(ColumnWithTypeAndName elem)
{
    if (elem.name.empty())
        throw Exception("Column name in Block cannot be empty", ErrorCodes::AMBIGUOUS_COLUMN_NAME);

    if (index_by_name.end() == index_by_name.find(elem.name))
        insert(std::move(elem));
}


void Block::erase(const std::set<size_t> & positions)
{
    for (auto it = positions.rbegin(); it != positions.rend(); ++it)
        erase(*it);
}


void Block::erase(size_t position)
{
    if (data.empty())
        throw Exception("Block is empty", ErrorCodes::POSITION_OUT_OF_BOUND);

    if (position >= data.size())
        throw Exception("Position out of bound in Block::erase(), max position = "
            + toString(data.size() - 1), ErrorCodes::POSITION_OUT_OF_BOUND);

    eraseImpl(position);
}


void Block::eraseImpl(size_t position)
{
    data.erase(data.begin() + position);

    for (auto it = index_by_name.begin(); it != index_by_name.end();)
    {
        if (it->second == position)
            it = index_by_name.erase(it);
        else
        {
            if (it->second > position)
                --it->second;
            ++it;
        }
    }
}


void Block::erase(const String & name)
{
    auto index_it = index_by_name.find(name);
    if (index_it == index_by_name.end())
        throw Exception("No such name in Block::erase(): '"
            + name + "'", ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);

    eraseImpl(index_it->second);
}


ColumnWithTypeAndName & Block::safeGetByPosition(size_t position)
{
    if (data.empty())
        throw Exception("Block is empty", ErrorCodes::POSITION_OUT_OF_BOUND);

    if (position >= data.size())
        throw Exception("Position " + toString(position)
            + " is out of bound in Block::safeGetByPosition(), max position = "
            + toString(data.size() - 1)
            + ", there are columns: " + dumpNames(), ErrorCodes::POSITION_OUT_OF_BOUND);

    return data[position];
}


const ColumnWithTypeAndName & Block::safeGetByPosition(size_t position) const
{
    if (data.empty())
        throw Exception("Block is empty", ErrorCodes::POSITION_OUT_OF_BOUND);

    if (position >= data.size())
        throw Exception("Position " + toString(position)
            + " is out of bound in Block::safeGetByPosition(), max position = "
            + toString(data.size() - 1)
            + ", there are columns: " + dumpNames(), ErrorCodes::POSITION_OUT_OF_BOUND);

    return data[position];
}


const ColumnWithTypeAndName * Block::findByName(const std::string & name, bool case_insensitive) const
{
    if (case_insensitive)
    {
        auto found = std::find_if(data.begin(), data.end(), [&](const auto & column) { return boost::iequals(column.name, name); });
        if (found == data.end())
        {
            return nullptr;
        }
        return &*found;
    }

    auto it = index_by_name.find(name);
    if (index_by_name.end() == it)
    {
        return nullptr;
    }
    return &data[it->second];
}


const ColumnWithTypeAndName & Block::getByName(const std::string & name, bool case_insensitive) const
{
    const auto * result = findByName(name, case_insensitive);
    if (!result)
        throw Exception(
            "Not found column " + name + " in block. There are only columns: " + dumpNames(), ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);

    return *result;
}


bool Block::has(const std::string & name, bool case_insensitive) const
{
    if (case_insensitive)
        return std::find_if(data.begin(), data.end(), [&](const auto & column) { return boost::iequals(column.name, name); })
            != data.end();

    return index_by_name.end() != index_by_name.find(name);
}


size_t Block::getPositionByName(const std::string & name) const
{
    auto it = index_by_name.find(name);
    if (index_by_name.end() == it)
        throw Exception(
            "Not found column " + name + " in block. There are only columns: " + dumpNames(), ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK);

    return it->second;
}

/// proton : starts
std::optional<size_t> Block::tryGetPositionByName(const std::string & name) const
{
    auto it = index_by_name.find(name);
    if (index_by_name.end() == it)
        return {};

    return it->second;
}
/// proton : ends

void Block::checkNumberOfRows(bool allow_null_columns) const
{
    ssize_t rows = -1;
    for (const auto & elem : data)
    {
        if (!elem.column && allow_null_columns)
            continue;

        if (!elem.column)
            throw Exception("Column " + elem.name + " in block is nullptr, in method checkNumberOfRows."
                , ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

        ssize_t size = elem.column->size();

        if (rows == -1)
            rows = size;
        else if (rows != size)
            throw Exception("Sizes of columns doesn't match: "
                + data.front().name + ": " + toString(rows)
                + ", " + elem.name + ": " + toString(size)
                , ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);
    }
}


size_t Block::rows() const
{
    for (const auto & elem : data)
        if (elem.column)
            return elem.column->size();

    return 0;
}


size_t Block::bytes() const
{
    size_t res = 0;
    for (const auto & elem : data)
        res += elem.column->byteSize();

    return res;
}

size_t Block::allocatedBytes() const
{
    return allocatedMetadataBytes() + allocatedDataBytes();
}

size_t Block::allocatedDataBytes() const
{
    size_t res = 0;
    for (const auto & elem : data)
        res += elem.column->allocatedBytes();

    return res;
}

size_t Block::allocatedMetadataBytes() const
{
    size_t res = 0;
    for (const auto & elem : data)
        res += elem.column->allocatedMetadataBytes();

    res += sizeof(data) + data.capacity() * sizeof(ColumnWithTypeAndName);
    res += sizeof(index_by_name) + index_by_name.size() * sizeof(IndexByName::value_type);
    res += sizeof(info);
    return res;
}

std::string Block::dumpNames() const
{
    WriteBufferFromOwnString out;
    for (auto it = data.begin(); it != data.end(); ++it)
    {
        if (it != data.begin())
            out << ", ";
        out << it->name;
    }
    return out.str();
}


std::string Block::dumpStructure() const
{
    WriteBufferFromOwnString out;
    for (auto it = data.begin(); it != data.end(); ++it)
    {
        if (it != data.begin())
            out << ", ";
        it->dumpStructure(out);
    }
    return out.str();
}

std::string Block::dumpIndex() const
{
    WriteBufferFromOwnString out;
    bool first = true;
    for (const auto & [name, pos] : index_by_name)
    {
        if (!first)
            out << ", ";
        first = false;

        out << name << ' ' << pos;
    }
    return out.str();
}

Block Block::cloneEmpty() const
{
    Block res;
    res.reserve(data.size());

    for (const auto & elem : data)
        res.insert(elem.cloneEmpty());

    return res;
}


MutableColumns Block::cloneEmptyColumns() const
{
    size_t num_columns = data.size();
    MutableColumns columns(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
        columns[i] = data[i].column ? data[i].column->cloneEmpty() : data[i].type->createColumn();
    return columns;
}


Columns Block::getColumns() const
{
    size_t num_columns = data.size();
    Columns columns(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
        columns[i] = data[i].column;
    return columns;
}


MutableColumns Block::mutateColumns()
{
    size_t num_columns = data.size();
    MutableColumns columns(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
        columns[i] = data[i].column ? IColumn::mutate(std::move(data[i].column)) : data[i].type->createColumn();
    return columns;
}


void Block::setColumns(MutableColumns && columns)
{
    /// TODO: assert if |columns| doesn't match |data|!
    size_t num_columns = data.size();
    for (size_t i = 0; i < num_columns; ++i)
        data[i].column = std::move(columns[i]);
}


void Block::setColumns(const Columns & columns)
{
    /// TODO: assert if |columns| doesn't match |data|!
    size_t num_columns = data.size();
    for (size_t i = 0; i < num_columns; ++i)
        data[i].column = columns[i];
}


void Block::setColumn(size_t position, ColumnWithTypeAndName column)
{
    if (position >= data.size())
        throw Exception(ErrorCodes::POSITION_OUT_OF_BOUND, "Position {} out of bound in Block::setColumn(), max position {}",
                        position, toString(data.size()));

    if (data[position].name != column.name)
    {
        index_by_name.erase(data[position].name);
        index_by_name.emplace(column.name, position);
    }

    data[position] = std::move(column);
}


Block Block::cloneWithColumns(MutableColumns && columns) const
{
    Block res;

    size_t num_columns = data.size();
    res.reserve(num_columns);

    for (size_t i = 0; i < num_columns; ++i)
        res.insert({ std::move(columns[i]), data[i].type, data[i].name });

    return res;
}


Block Block::cloneWithColumns(const Columns & columns) const
{
    Block res;

    size_t num_columns = data.size();

    if (num_columns != columns.size())
        throw Exception("Cannot clone block with columns because block has " + toString(num_columns) + " columns, "
                        "but " + toString(columns.size()) + " columns given.", ErrorCodes::LOGICAL_ERROR);

    res.reserve(num_columns);

    for (size_t i = 0; i < num_columns; ++i)
        res.insert({ columns[i], data[i].type, data[i].name });

    return res;
}


Block Block::cloneWithoutColumns() const
{
    Block res;

    size_t num_columns = data.size();
    res.reserve(num_columns);

    for (size_t i = 0; i < num_columns; ++i)
        res.insert({ nullptr, data[i].type, data[i].name });

    return res;
}

Block Block::cloneWithCutColumns(size_t start, size_t length) const
{
    Block copy = *this;

    for (auto & column_to_cut : copy.data)
        column_to_cut.column = column_to_cut.column->cut(start, length);

    return copy;
}

Block Block::sortColumns() const
{
    Block sorted_block;

    /// std::unordered_map (index_by_name) cannot be used to guarantee the sort order
    std::vector<IndexByName::const_iterator> sorted_index_by_name(index_by_name.size());
    {
        size_t i = 0;
        for (auto it = index_by_name.begin(); it != index_by_name.end(); ++it)
            sorted_index_by_name[i++] = it;
    }
    ::sort(sorted_index_by_name.begin(), sorted_index_by_name.end(), [](const auto & lhs, const auto & rhs)
    {
        return lhs->first < rhs->first;
    });

    for (const auto & it : sorted_index_by_name)
        sorted_block.insert(data[it->second]);

    return sorted_block;
}


const ColumnsWithTypeAndName & Block::getColumnsWithTypeAndName() const
{
    return data;
}


NamesAndTypesList Block::getNamesAndTypesList() const
{
    NamesAndTypesList res;

    for (const auto & elem : data)
        res.emplace_back(elem.name, elem.type);

    return res;
}

NamesAndTypes Block::getNamesAndTypes() const
{
    NamesAndTypes res;

    for (const auto & elem : data)
        res.emplace_back(elem.name, elem.type);

    return res;
}

Names Block::getNames() const
{
    Names res;
    res.reserve(columns());

    for (const auto & elem : data)
        res.push_back(elem.name);

    return res;
}


DataTypes Block::getDataTypes() const
{
    DataTypes res;
    res.reserve(columns());

    for (const auto & elem : data)
        res.push_back(elem.type);

    return res;
}

Names Block::getDataTypeNames() const
{
    Names res;
    res.reserve(columns());

    for (const auto & elem : data)
        res.push_back(elem.type->getName());

    return res;
}

std::unordered_map<String, size_t> Block::getNamesToIndexesMap() const
{
    return index_by_name;
}

bool blocksHaveEqualStructure(const Block & lhs, const Block & rhs)
{
    return checkBlockStructure<bool>(lhs, rhs, "", false, /*ignore_name=*/false);
}


void assertBlocksHaveEqualStructure(const Block & lhs, const Block & rhs, std::string_view context_description)
{
    checkBlockStructure<void>(lhs, rhs, context_description, false, /*ignore_name=*/false);
}


bool isCompatibleHeader(const Block & actual, const Block & desired)
{
    return checkBlockStructure<bool>(actual, desired, "", true, /*ignore_name=*/false);
}


void assertCompatibleHeader(const Block & actual, const Block & desired, std::string_view context_description)
{
    checkBlockStructure<void>(actual, desired, context_description, true, /*ignore_name=*/false);
}

bool isCompatibleHeaderWithoutComparingColumnNames(const Block & actual, const Block & desired)
{
    return checkBlockStructure<bool>(actual, desired, "", true, /*ignore_name=*/true);
}

void getBlocksDifference(const Block & lhs, const Block & rhs, std::string & out_lhs_diff, std::string & out_rhs_diff)
{
    /// The traditional task: the largest common subsequence (LCS).
    /// Assume that order is important. If this becomes wrong once, let's simplify it: for example, make 2 sets.

    std::vector<std::vector<int>> lcs(lhs.columns() + 1);
    for (auto & v : lcs)
        v.resize(rhs.columns() + 1);

    for (size_t i = 1; i <= lhs.columns(); ++i)
    {
        for (size_t j = 1; j <= rhs.columns(); ++j)
        {
            if (lhs.safeGetByPosition(i - 1) == rhs.safeGetByPosition(j - 1))
                lcs[i][j] = lcs[i - 1][j - 1] + 1;
            else
                lcs[i][j] = std::max(lcs[i - 1][j], lcs[i][j - 1]);
        }
    }

    /// Now go back and collect the answer.
    ColumnsWithTypeAndName left_columns;
    ColumnsWithTypeAndName right_columns;
    size_t l = lhs.columns();
    size_t r = rhs.columns();
    while (l > 0 && r > 0)
    {
        if (lhs.safeGetByPosition(l - 1) == rhs.safeGetByPosition(r - 1))
        {
            /// This element is in both sequences, so it does not get into `diff`.
            --l;
            --r;
        }
        else
        {
            /// Small heuristics: most often used when getting a difference for (expected_block, actual_block).
            /// Therefore, the preference will be given to the field, which is in the left block (expected_block), therefore
            /// in `diff` the column from `actual_block` will get.
            if (lcs[l][r - 1] >= lcs[l - 1][r])
                right_columns.push_back(rhs.safeGetByPosition(--r));
            else
                left_columns.push_back(lhs.safeGetByPosition(--l));
        }
    }

    while (l > 0)
        left_columns.push_back(lhs.safeGetByPosition(--l));
    while (r > 0)
        right_columns.push_back(rhs.safeGetByPosition(--r));

    WriteBufferFromString lhs_diff_writer(out_lhs_diff);
    WriteBufferFromString rhs_diff_writer(out_rhs_diff);

    for (auto it = left_columns.rbegin(); it != left_columns.rend(); ++it)
    {
        lhs_diff_writer << it->dumpStructure();
        lhs_diff_writer << ", position: " << lhs.getPositionByName(it->name) << '\n';
    }
    for (auto it = right_columns.rbegin(); it != right_columns.rend(); ++it)
    {
        rhs_diff_writer << it->dumpStructure();
        rhs_diff_writer << ", position: " << rhs.getPositionByName(it->name) << '\n';
    }
}


void Block::clear()
{
    info = BlockInfo();
    data.clear();
    index_by_name.clear();
}

void Block::swap(Block & other) noexcept
{
    std::swap(info, other.info);
    data.swap(other.data);
    index_by_name.swap(other.index_by_name);
}


void Block::updateHash(SipHash & hash) const
{
    for (size_t row_no = 0, num_rows = rows(); row_no < num_rows; ++row_no)
        for (const auto & col : data)
            col.column->updateHashWithValue(row_no, hash);
}

Serializations Block::getSerializations() const
{
    Serializations res;
    res.reserve(data.size());

    for (const auto & column : data)
        res.push_back(column.type->getDefaultSerialization());

    return res;
}

void convertToFullIfSparse(Block & block)
{
    for (auto & column : block)
        column.column = recursiveRemoveSparse(column.column);
}

Block materializeBlock(const Block & block)
{
    if (!block)
        return block;

    Block res = block;
    size_t columns = res.columns();
    for (size_t i = 0; i < columns; ++i)
    {
        auto & element = res.getByPosition(i);
        element.column = recursiveRemoveSparse(element.column->convertToFullColumnIfConst());
    }

    return res;
}

void materializeBlockInplace(Block & block)
{
    for (size_t i = 0; i < block.columns(); ++i)
        block.getByPosition(i).column = recursiveRemoveSparse(block.getByPosition(i).column->convertToFullColumnIfConst());
}

Block concatenateBlocks(const std::vector<Block> & blocks)
{
    if (blocks.empty())
        return {};

    size_t num_rows = 0;
    for (const auto & block : blocks)
        num_rows += block.rows();

    Block out = blocks[0].cloneEmpty();
    MutableColumns columns = out.mutateColumns();

    for (size_t i = 0; i < columns.size(); ++i)
    {
        columns[i]->reserve(num_rows);
        for (const auto & block : blocks)
        {
            /// proton : starts, check block has rows
            if (block.rows() > 0)
            {
                const auto & tmp_column = *block.getByPosition(i).column;
                columns[i]->insertRangeFrom(tmp_column, 0, block.rows());
            }
            /// proton : ends
        }
    }

    out.setColumns(std::move(columns));
    return out;
}

/// proton: starts
void Block::reserve(size_t num_columns)
{
    index_by_name.reserve(num_columns);
    data.reserve(num_columns);
}

void Block::reorderColumnsInplace(const std::vector<UInt16> & positions)
{
    if (positions.size() <= 1)
        return;

    auto num_columns = columns();
    assert (positions.size() == num_columns);

    /// FIXME: a better algorithm
    // size_t exchanged = 0;
    // for (size_t i = 0; exchanged < num_columns - 1 && i < num_columns;)
    // {
    //     auto target_pos = positions[i];
    //     if (i != target_pos)
    //     {
    //         /// Move column at positions[pos] to pos
    //         assert(target_pos < num_columns);
    //         data[i].swap(data[target_pos]);
    //         ++exchanged;
    //     }
    //     else
    //         ++i;
    // }

    // if (exchanged)
    // {
    //     /// Correct index_by_name
    //     for (size_t i = 0; const auto & col : data)
    //     {
    //         index_by_name[col.name] = i;
    //         ++i;
    //     }
    // }

    Block result;
    result.reserve(num_columns);

    for (auto target_pos : positions)
    {
        auto & target_col = getByPosition(target_pos);
        result.insert(std::move(target_col));
    }

    /// Don't swap block.info
    data.swap(result.data);
    index_by_name.swap(result.index_by_name);
}

void Block::renameColumn(String new_name, size_t column_pos)
{
    assert(column_pos < data.size());
    assert(!has(new_name));

    auto & col_with_name = getByPosition(column_pos);
    index_by_name.erase(col_with_name.name);
    col_with_name.name = std::move(new_name);
    index_by_name.emplace(col_with_name.name, column_pos);
}

Block Block::deepClone() const
{
    Block result;
    result.reserve(columns());
    for (const auto & col : *this)
        result.insert(ColumnWithTypeAndName{col.column->cloneResized(col.column->size()), col.type, col.name});

    return result;
}

bool Block::hasDynamicSubcolumns() const
{
    for (const auto & col : *this)
        if (col.type->hasDynamicSubcolumns())
            return true;

    return false;
}

void Block::reorderColumnsInplace(const Block & header)
{
    assert(header.columns() >= 1);

    auto num_columns = columns();
    if (num_columns <= 1)
        return;

    assert(num_columns >= header.columns());

    /// FIXME: a better algorithm
    // if (num_columns == header.columns())
    // {
    //     /// Fast
    //     size_t exchanged = 0;
    //     for (size_t i = 0; exchanged < num_columns - 1 && i < num_columns;)
    //     {
    //         auto target_pos = header.getPositionByName(data[i].name);
    //         if (i != target_pos)
    //         {
    //             data[i].swap(data[target_pos]);
    //             ++exchanged;
    //         }
    //         else
    //             ++i;
    //     }

    //     /// Correct index_by_name
    //     if (exchanged)
    //     {
    //         for (size_t i = 0; const auto & col : data)
    //         {
    //             index_by_name[col.name] = i;
    //             ++i;
    //         }
    //     }
    // }
    // else
    // {
        Block result;
        result.reserve(header.columns());

        for (const auto & col : header)
        {
            auto & target_col = getByName(col.name);
            result.insert(std::move(target_col));
        }

        /// Don't swap block.info
        data.swap(result.data);
        index_by_name.swap(result.index_by_name);
    // }
}

void Block::reorderColumnsInplace(const Names & names)
{
    assert(names.size() >= 1);

    auto num_columns = columns();
    if (num_columns <= 1)
        return;

    assert(num_columns >= names.size());

    Block result;
    result.reserve(names.size());

    for (const auto & name : names)
    {
        auto & target_col = getByName(name);
        result.insert(std::move(target_col));
    }

    /// Don't swap block.info
    data.swap(result.data);
    index_by_name.swap(result.index_by_name);
}

void Block::concat(const Block & other)
{
    assert(blocksHaveEqualStructure(*this, other));

    auto added_rows = other.rows();
    assert(added_rows != 0);

    for (size_t col_pos = 0; auto & col : data)
        col.column->assumeMutable()->insertRangeFrom(*other.data[col_pos++].column, 0, added_rows);
}

void Block::insertRow(size_t row_num, Block & target_block) const
{
    assert(row_num < rows());
    assert(blocksHaveEqualStructure(*this, target_block));

    for (size_t col_pos = 0; const auto & col : data)
        target_block.data[col_pos++].column->assumeMutable()->insertFrom(*col.column, row_num);
}

int Block::compareAt(size_t lhs_row, size_t rhs_row, const Block & rhs_block, const std::vector<size_t> & skip_columns) const
{
    assert(columns() == rhs_block.columns());

    for (size_t i = 0, num_columns = columns(); i < num_columns; ++i)
    {
        if (std::find(skip_columns.begin(), skip_columns.end(), i) != skip_columns.end())
            continue;

        const auto & lhs_col = getByPosition(i);
        const auto & rhs_col = rhs_block.getByPosition(i);

        if (auto r = lhs_col.column->compareAt(lhs_row, rhs_row, *rhs_col.column, -1); r != 0)
            return r;
    }

    return 0;
}

Columns Block::detachColumns()
{
    size_t num_columns = data.size();
    Columns columns(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
        columns[i] = std::move(data[i].column);

    clear();
    return columns;
}
/// proton: ends

}
