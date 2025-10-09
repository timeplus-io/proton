#pragma once

#include <Processors/Formats/Impl/JSONEachRowRowOutputFormat.h>

namespace DB
{

/// The stream for outputting data formatted with the OpenSearch bulk API request body format (create only):
/// https://docs.opensearch.org/docs/latest/api-reference/document-apis/bulk/#request-body
class OpenSearchRowOutputFormat : public JSONEachRowRowOutputFormat
{
public:
    OpenSearchRowOutputFormat(
        WriteBuffer & out_,
        const Block & header_,
        const FormatSettings & settings_,
        ProcessorID pid_ = ProcessorID::OpenSearchRowOutputFormatID);

    String getName() const override { return "OpenSearchRowOutputFormat"; }

    void write(const Columns & columns, size_t row_num) override;

protected:
    void writeField(const IColumn & column, const ISerialization & serialization, size_t row_num) override;
    void writeFieldDelimiter() override;

private:
    std::function<void(const Columns & columns, size_t row_num)> write_index{nullptr};
    std::function<void(const Columns & columns, size_t row_num)> write_id{nullptr};

    std::optional<size_t> index_col_pos;
    std::optional<size_t> id_col_pos;
    bool skip_field{false};
};

}
