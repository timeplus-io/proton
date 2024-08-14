#pragma once

#include <Core/BlockInfo.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/NamesAndTypes.h>

#include <initializer_list>
#include <list>
#include <map>
#include <set>
#include <vector>

namespace DB
{

/** Container for set of columns for bunch of rows in memory.
  * This is unit of data processing.
  * Also contains metadata - data types of columns and their names
  *  (either original names from a table, or generated names during temporary calculations).
  * Allows to insert, remove columns in arbitrary position, to change order of columns.
  */

class Block
{
private:
    using Container = ColumnsWithTypeAndName;
    using IndexByName = std::unordered_map<String, size_t>;

    Container data;
    IndexByName index_by_name;

public:
    BlockInfo info;

    Block() = default;
    Block(std::initializer_list<ColumnWithTypeAndName> il);
    Block(const ColumnsWithTypeAndName & data_);
    Block(ColumnsWithTypeAndName && data_);

    /// insert the column at the specified position
    void insert(size_t position, ColumnWithTypeAndName elem);
    /// insert the column to the end
    void insert(ColumnWithTypeAndName elem);
    /// insert the column to the end, if there is no column with that name yet
    void insertUnique(ColumnWithTypeAndName elem);
    /// remove the column at the specified position
    void erase(size_t position);
    /// remove the columns at the specified positions
    void erase(const std::set<size_t> & positions);
    /// remove the column with the specified name
    void erase(const String & name);

    /// References are invalidated after calling functions above.

    ColumnWithTypeAndName & getByPosition(size_t position) { return data[position]; }
    const ColumnWithTypeAndName & getByPosition(size_t position) const { return data[position]; }

    ColumnWithTypeAndName & safeGetByPosition(size_t position);
    const ColumnWithTypeAndName & safeGetByPosition(size_t position) const;

    ColumnWithTypeAndName* findByName(const std::string & name, bool case_insensitive = false)
    {
        return const_cast<ColumnWithTypeAndName *>(
            const_cast<const Block *>(this)->findByName(name, case_insensitive));
    }

    const ColumnWithTypeAndName * findByName(const std::string & name, bool case_insensitive = false) const;

    ColumnWithTypeAndName & getByName(const std::string & name, bool case_insensitive = false)
    {
        return const_cast<ColumnWithTypeAndName &>(
            const_cast<const Block *>(this)->getByName(name, case_insensitive));
    }

    const ColumnWithTypeAndName & getByName(const std::string & name, bool case_insensitive = false) const;

    Container::iterator begin() { return data.begin(); }
    Container::iterator end() { return data.end(); }
    Container::const_iterator begin() const { return data.begin(); }
    Container::const_iterator end() const { return data.end(); }
    Container::const_iterator cbegin() const { return data.cbegin(); }
    Container::const_iterator cend() const { return data.cend(); }

    bool has(const std::string & name, bool case_insensitive = false) const;

    size_t getPositionByName(const std::string & name) const;

    /// proton : starts
    std::optional<size_t> tryGetPositionByName(const std::string & name) const;
    /// proton : ends

    const ColumnsWithTypeAndName & getColumnsWithTypeAndName() const;
    NamesAndTypesList getNamesAndTypesList() const;
    NamesAndTypes getNamesAndTypes() const;
    Names getNames() const;
    DataTypes getDataTypes() const;
    Names getDataTypeNames() const;

    /// Returns number of rows from first column in block, not equal to nullptr. If no columns, returns 0.
    size_t rows() const;

    size_t columns() const { return data.size(); }

    /// Checks that every column in block is not nullptr and has same number of elements.
    void checkNumberOfRows(bool allow_null_columns = false) const;

    /// Approximate number of bytes in memory - for profiling and limits.
    size_t bytes() const;

    /// Approximate number of allocated bytes in memory - for profiling and limits.
    size_t allocatedBytes() const;
    size_t allocatedDataBytes() const;
    size_t allocatedMetadataBytes() const;

    operator bool() const { return !!columns(); }
    bool operator!() const { return !this->operator bool(); }

    /** Get a list of column names separated by commas. */
    std::string dumpNames() const;

    /** List of names, types and lengths of columns. Designed for debugging. */
    std::string dumpStructure() const;

    /** List of column names and positions from index */
    std::string dumpIndex() const;

    /** Get the same block, but empty. */
    Block cloneEmpty() const;

    Columns getColumns() const;
    void setColumns(const Columns & columns);
    void setColumn(size_t position, ColumnWithTypeAndName column);
    Block cloneWithColumns(const Columns & columns) const;
    Block cloneWithoutColumns() const;
    Block cloneWithCutColumns(size_t start, size_t length) const;

    /** Get empty columns with the same types as in block. */
    MutableColumns cloneEmptyColumns() const;

    /** Get columns from block for mutation. Columns in block will be nullptr. */
    MutableColumns mutateColumns();

    /** Replace columns in a block */
    void setColumns(MutableColumns && columns);
    Block cloneWithColumns(MutableColumns && columns) const;

    /** Get a block with columns that have been rearranged in the order of their names. */
    Block sortColumns() const;

    void clear();
    void swap(Block & other) noexcept;

    /** Updates SipHash of the Block, using update method of columns.
      * Returns hash for block, that could be used to differentiate blocks
      *  with same structure, but different data.
      */
    void updateHash(SipHash & hash) const;

    std::unordered_map<String, size_t> getNamesToIndexesMap() const;
    Serializations getSerializations() const;

    /// proton: starts
    void reserve(size_t num_columns);

    /// in-place sort columns according to positions
    void reorderColumnsInplace(const std::vector<UInt16> & positions);

    /// in-place sort columns according to header
    void reorderColumnsInplace(const Block & header);

    /// in-place sort columns according to names
    void reorderColumnsInplace(const Names & names);

    void renameColumn(String new_name, size_t column_pos);

    Columns detachColumns();

    /// Deep clone, use cautiously. Most of time, we don't need deepClone
    Block deepClone() const;

    bool hasWatermark() const { return info.hasWatermark(); }

    bool hasDynamicSubcolumns() const;

    /// In-place concat other block to the current block
    void concat(const Block & other);

    /// Copy row to target_block
    void insertRow(size_t row_num, Block & target_block) const;
    int compareAt(size_t lhs_row, size_t rhs_row, const Block & rhs_block, const std::vector<size_t> & skip_columns) const;
    /// Reuse BlockInfo.watermark for min/max timestamp
    Int64 minTimestamp() const noexcept { return info.watermark_lower_bound; }
    Int64 maxTimestamp() const noexcept { return info.watermark; }
    Int64 watermark() const noexcept { return info.watermark; }
    void setMinTimestamp(Int64 min_ts) noexcept { info.watermark_lower_bound = min_ts; }
    void setMaxTimestamp(Int64 max_ts) noexcept { info.watermark = max_ts; }
    /// proton: ends

private:
    void eraseImpl(size_t position);
    void initializeIndexByName();

    /// This is needed to allow function execution over data.
    /// It is safe because functions does not change column names, so index is unaffected.
    /// It is temporary.
    friend class ExpressionActions;
    friend class ActionsDAG;
};

using BlockPtr = std::shared_ptr<Block>;
using Blocks = std::vector<Block>;
using BlocksList = std::list<Block>;
using BlocksPtr = std::shared_ptr<Blocks>;
using BlocksPtrs = std::shared_ptr<std::vector<BlocksPtr>>;

/// Extends block with extra data in derived classes
struct ExtraBlock
{
    Block block;

    bool empty() const { return !block; }
};

using ExtraBlockPtr = std::shared_ptr<ExtraBlock>;

/// Compare number of columns, data types, column types, column names, and values of constant columns.
bool blocksHaveEqualStructure(const Block & lhs, const Block & rhs);

/// Throw exception when blocks are different.
void assertBlocksHaveEqualStructure(const Block & lhs, const Block & rhs, std::string_view context_description);

/// Actual header is compatible to desired if block have equal structure except constants.
/// It is allowed when column from actual header is constant, but in desired is not.
/// If both columns are constant, it is checked that they have the same value.
bool isCompatibleHeader(const Block & actual, const Block & desired);
void assertCompatibleHeader(const Block & actual, const Block & desired, std::string_view context_description);

/// proton : starts. Introduce head structure comparision without comparing the column names
/// Actual header is compatible to desired if block have equal structure except column names
bool isCompatibleHeaderWithoutComparingColumnNames(const Block & actual, const Block & desired);
/// proton : ends

/// Calculate difference in structure of blocks and write description into output strings. NOTE It doesn't compare values of constant columns.
void getBlocksDifference(const Block & lhs, const Block & rhs, std::string & out_lhs_diff, std::string & out_rhs_diff);

void convertToFullIfSparse(Block & block);

/// Converts columns-constants to full columns ("materializes" them).
Block materializeBlock(const Block & block);
void materializeBlockInplace(Block & block);

Block concatenateBlocks(const std::vector<Block> & blocks);

}
