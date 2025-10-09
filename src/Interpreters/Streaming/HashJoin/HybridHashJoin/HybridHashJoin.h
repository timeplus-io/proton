#include <Core/LightChunk.h>
#include <Interpreters/Streaming/HashJoin/HashJoin.h>
#include <Interpreters/Streaming/HashJoin/HybridHashJoin/HashIndex.h>
#include <Interpreters/Streaming/HashJoin/joinKind.h>
#include <Interpreters/TableJoin.h>
#include <Common/HybridHashTable/HybridHashTableTemplate.h>
#include <Common/serde.h>

#include <boost/container_hash/hash.hpp>

namespace DB
{
using NullMap = ColumnUInt8::Container;
using ConstNullMapPtr = const NullMap *;

namespace Streaming
{
class HybridHashJoin final : public HashJoin
{
public:
    HybridHashJoin(
        std::shared_ptr<TableJoin> table_join_,
        JoinStreamDescriptionPtr left_join_stream_desc_,
        JoinStreamDescriptionPtr right_join_stream_desc_,
        const String & spill_dir_,
        size_t max_hot_key_count_);

    ~HybridHashJoin() noexcept override;

    HashJoinType type() const noexcept override { return HashJoinType::Hybrid; }

    void postInit(const Block & left_header, const Block & output_header_, UInt64 join_max_cached_bytes_) override;

    void transformHeader(Block & header) override;

    /// For non-bidirectional hash join
    void insertRightBlock(Block right_block) override;
    void joinLeftBlock(Block & left_block) override;

    /// For bidirectional hash join
    /// There are 2 blocks returned : joined block via parameter and retracted block via returned-value if there is
    Block insertLeftBlockAndJoin(Block & left_block) override;
    Block insertRightBlockAndJoin(Block & right_block) override;

    /// For bidirectional range hash join, there may be multiple joined blocks
    std::vector<Block> insertLeftBlockToRangeBucketsAndJoin(Block left_block) override;
    std::vector<Block> insertRightBlockToRangeBucketsAndJoin(Block right_block) override;

    /// "Legacy API", use insertRightBlock()
    bool addJoinedBlock(const Block & block, bool check_limits) override;

    /// "Legacy API", use joinLeftBlock()
    void joinBlock(Block & block, ExtraBlockPtr & not_processed) override;

    IBlocksStreamPtr
    getNonJoinedBlocks(const Block & left_sample_block, const Block & result_sample_block, UInt64 max_block_size) const override;

    String metricsString() const override;

    void serialize(WriteBuffer & wb, VersionType version) const override;
    void deserialize(ReadBuffer & rb, VersionType version) override;

    /// Write / read data to / from RocksDB
    void write(VersionType version);
    void read(VersionType version);

    void cancel() override;

    size_t getTotalRowCount() const override;
    /// Sum size in bytes of all buffers, used for JOIN maps and for all memory pools.
    size_t getTotalByteCount() const override;

    bool alwaysReturnsEmptySet() const override;

    RocksPtr getOrCreateRocks();
    void shutdownRocks();
    const String & getRocksDir() const { return base_config.spill_dir_path; }

    void reinstallRocks();

private:
    struct JoinData
    {
        explicit JoinData(JoinContext & join_ctx_) : join_ctx(join_ctx_) { }

        JoinContext & join_ctx;
        SERDE HashIndexPtr index;
    };

    void installRocks(const String & spill_dir_, size_t max_hot_key_count_);

    void chooseHashMethod();
    void checkLimits() const;
    void initHashIndexes();

    void validateAsofJoinKey();
    void initHashMaps(HybridHashJoinMapsVariants & all_maps, const Block & sample_block_to_save, std::string_view table_id);

    size_t dataBlockSize() const noexcept { return table_join->dataBlockSize(); }

    /// If the left / right_block is a retraction block : rows in `_tp_delta` column all have `-1`
    /// We erase the previous key / values from hash table
    /// Use for append JOIN changelog_kv / versioned_kv case
    /// \return true if keys are erased, otherwise false
    template <bool is_left_block>
    void eraseExistingKeys(Block & block, JoinData & join_data);

    template <typename KeyGetter, typename TaggedType, typename Map>
    void doEraseExistingKeys(
        Map & map,
        const Block & saved_block,
        ColumnRawPtrs && key_columns,
        const std::vector<size_t> & key_size,
        const std::vector<size_t> & skip_columns,
        ConstNullMapPtr null_map,
        bool delete_key);

    template <bool is_left_block>
    std::optional<Block> eraseExistingKeysAndRetractJoin(Block & block);

    template <bool is_left_block>
    void doInsertBlock(Block block, HybridHashJoinMapsVariants & maps_variants, IColumn::Filter * new_keys_filter = nullptr);

    template <bool is_left_block>
    std::vector<Block> insertBlockToRangeBucketsAndJoin(Block block);

    template <bool is_left_block>
    void transformToOutputBlock(Block & joined_block) const;

    template <bool is_left_block>
    Block joinBlockWithHashTable(Block & block, HybridHashJoinMapsVariants & maps_variants);

    /// Provide an API to join with join_kind instead of using member variable streaming_kind
    template <bool is_left_block>
    Block joinBlockWithHashTable(Block & block, HybridHashJoinMapsVariants & maps_variants, Kind join_kind);

    template <bool is_left_block>
    void doJoinBlockWithHashTable(Block & block, HybridHashJoinMapsVariants & maps_variants);

    template <bool is_left_block>
    void doJoinBlockWithHashTable(Block & block, HybridHashJoinMapsVariants & maps_variants, Kind join_kind);

    template <bool is_left_block>
    void doJoinBlockWithHashTables(Block & block, std::vector<HybridHashJoinMapsVariants *> & maps_variants_v, Kind join_kind);

    template <bool is_left_block, Kind KIND, Strictness STRICTNESS, typename TaggedMap>
    void joinBlockImpl(Block & block, const Block & block_with_columns_to_add, std::vector<std::vector<TaggedMap *>> & map_vv) const;

    void calculateWatermark();

    /// For changelog emit
    struct JoinResults
    {
        JoinResults(const Block & header_) : sample_block(header_), index(std::make_unique<HybridHashJoinMapsVariants>()) { }

        mutable std::mutex mutex;

        Block sample_block;

        /// Building hash map for joined blocks, then we can find previous
        /// join blocks quickly by using joined keys
        SERDE std::unique_ptr<HybridHashJoinMapsVariants> index;
    };

private:
    friend struct HashIndex;

    HybridHashTableConfig base_config;
    RocksPtr rocks;

    /// Note: when left block joins right hashtable, use `right_data`
    SERDE JoinData right_data;
    /// Note: when right block joins left hashtable, use `left_data`
    SERDE JoinData left_data;

    SERDE HybridHashType hash_method_type;

    bool has_nullable_key = false;
    bool has_low_cardinality_key = false;

    /// Only SERDE when emit_changelog is true
    SERDE std::optional<JoinResults> join_results;
};

void serializeHybridHashJoinMapsVariants(
    const HybridHashJoinMapsVariants & indexes, WriteBuffer & wb, VersionType version, const HybridHashJoin & join);
void deserializeHybridHashJoinMapsVariants(
    HybridHashJoinMapsVariants & indexes, ReadBuffer & rb, VersionType version, const HybridHashJoin & join);

void writeHybridHashJoinMapsVariants(
    HybridHashJoinMapsVariants & indexes, RocksHandlerPtr rocks_handler, std::string_view prefix_id, const HybridHashJoin & join);
void readHybridHashJoinMapsVariants(
    HybridHashJoinMapsVariants & indexes, RocksHandlerPtr rocks_handler, std::string_view prefix_id, const HybridHashJoin & join);
}
}
