#pragma once

#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/Streaming/TableFunctionDescription_fwd.h>
#include <Processors/ISimpleTransform.h>
#include <Common/HashTable/Hash.h>
#include <Common/HybridHashTable/HybridHashTable.h>
#include <Common/serde.h>

namespace DB::Streaming
{

/// HybridDedupTransform dedup input column(s) according to the dedup keys
class HybridDedupTransform final : public ISimpleTransform
{
public:
    HybridDedupTransform(
        const Block & input_header,
        const Block & output_header,
        TableFunctionDescriptionPtr dedup_func_desc_,
        const String & spill_dir,
        const String & kv_options);

    ~HybridDedupTransform() override = default;

    String getName() const override { return "HybridDedupTransform"; }

    void transform(Chunk & chunk) override;

    bool hasState() const override { return true; }
    void doCheckpoint(CheckpointContextPtr ckpt_ctx) override;
    void doRecover(CheckpointContextPtr ckpt_ctx) override;

private:
    /// Calculate the positions of columns required by timestamp expr
    void init(const Block & input_header, const Block & output_header, const String & spill_dir, const String & kv_options);
    void initKeySet(const String & spill_dir, const String & kv_options);
    IColumn::Filter populateKeySetsAndCalculateResults(const ColumnsWithTypeAndName & arguments, size_t & rows);

private:
    ContextPtr context;

    TableFunctionDescriptionPtr dedup_func_desc;

    Chunk chunk_header;

    std::vector<size_t> expr_column_positions;
    std::vector<ssize_t> output_column_positions;

    using Key = UInt128;
    using Set = HybridHashTable<Key, UInt128TrivialHash>;
    SERDE std::unique_ptr<Set> key_set;

    LoggerPtr logger;
};

}
