#pragma once
#include "config.h"
#if USE_AVRO

#include <Core/Block.h>
#include <Formats/FormatSchemaInfo.h>
#include <Formats/FormatSettings.h>
#include <IO/WriteBuffer.h>
#include <Processors/Formats/IRowOutputFormat.h>

#include <Encoder.hh>
#include <ValidSchema.hh>

namespace DB
{

class AvroSchemaSerializer
{
public:
    AvroSchemaSerializer(avro::ValidSchema valid_schema, const Block & header, WriteBuffer & out);
    void serializeRow(const Columns & columns, size_t row_num);
    void flush() { encoder->flush(); }

private:
    using SerializeFn = std::function<void(const IColumn & column, size_t row_num, avro::Encoder & encoder)>;

    SerializeFn createSerializeFn(const avro::NodePtr & node, const DataTypePtr & data_type) const;
    SerializeFn createDefaultNullUnionSerializeFn() const;
    SerializeFn createUnionSerializeFn(int index) const;

    struct Action
    {
        enum Type
        {
            Unknown,
            Serialize,
            Group,
            Array,
            Nested
        };
        Type type{Unknown};

        int target_column_idx{0};
        /// If union_idx has value, it means it's inside a union field.
        std::optional<int> union_idx;
        SerializeFn serialize_fn;

        /// Record | Union
        std::vector<Action> actions;

        /// For flattened Nested column
        std::vector<size_t> nested_column_indexes;
        std::vector<SerializeFn> nested_serializers;

        static Action serializeAction(int target_column_idx_, SerializeFn serialize_fn_, std::optional<int> union_idx_ = std::nullopt);
        static Action groupAction(std::vector<Action> field_actions, std::optional<int> union_idx_ = std::nullopt);
        static Action arrayAction(std::vector<Action> field_actions, std::optional<int> union_idx_ = std::nullopt);
        static Action nestedAction(
            std::vector<size_t> nested_column_indexes_, std::vector<SerializeFn> nested_serializers_, std::optional<int> union_idx_);

        void execute(const Columns & columns, size_t row_num, avro::Encoder & enc) const;

    private:
        void serializeNested(const Columns & columns, size_t row_num, avro::Encoder & encoder) const;
        /// Serialize a flattened array of union.
        void serializeArray(const Columns & columns, size_t row_num, avro::Encoder & encoder) const;
    };

    /// Populate actions by recursively traversing root schema
    std::optional<Action>
    createAction(const Block & header, const avro::NodePtr & node, const std::string & current_path = "", int union_index = -1);

    std::unique_ptr<avro::OutputStream> ostream;
    avro::EncoderPtr encoder;
    /// Serialize actions for a row
    std::vector<Action> row_actions;
};

/// The counter part of `AvroConfluentRowInputFormat`.
class AvroConfluentRowOutputFormat final : public IRowOutputFormat
{
public:
    AvroConfluentRowOutputFormat(
        WriteBuffer & out_, const Block & header_, const RowOutputFormatParams & params_, const FormatSettings & settings_);
    ~AvroConfluentRowOutputFormat() override;

    String getName() const override { return "AvroConfluentRowOutputFormat"; }
    void consume(DB::Chunk chunk) override;
    void flush() override { serializer->flush(); }

protected:
    void consumeTotals(Chunk) override { }
    void consumeExtremes(Chunk) override { }

    void write(const Columns & columns, size_t row_num) override;
    void writeField(const IColumn &, const ISerialization &, size_t) override { }

private:
    FormatSettings settings;
    UInt32 schema_id{0};
    std::unique_ptr<AvroSchemaSerializer> serializer;
};

class AvroSchemaRowOutputFormat final : public IRowOutputFormat
{
public:
    AvroSchemaRowOutputFormat(WriteBuffer & out_, const Block & header_, const RowOutputFormatParams & params_, const FormatSchemaInfo & schema_info, const FormatSettings & format_settings);
    ~AvroSchemaRowOutputFormat() override;

    String getName() const override { return "AvroSchemaRowOutputFormat"; }
    void consume(DB::Chunk chunk) override;
    void flush() override { serializer->flush(); }

protected:
    void consumeTotals(Chunk) override { }
    void consumeExtremes(Chunk) override { }

    void write(const Columns & columns, size_t row_num) override;
    void writeField(const IColumn &, const ISerialization &, size_t) override { }

private:
    FormatSettings settings;
    std::unique_ptr<AvroSchemaSerializer> serializer;
};

}
#endif
