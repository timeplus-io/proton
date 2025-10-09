#include <Storages/ExternalStream/ExternalStreamSource.h>

#include <Formats/FormatFactory.h>
#include <Processors/Formats/RowInputFormatWithNamesAndTypes.h>

namespace DB
{

ExternalStreamSource::ExternalStreamSource(
    const Block & header_, StorageSnapshotPtr storage_snapshot_, size_t max_block_size_, ContextPtr query_context_)
    : header(header_)
    , storage_snapshot(std::move(storage_snapshot_))
    , header_chunk(Chunk(header_.getColumns(), 0))
    , max_block_size(max_block_size_)
    , read_buffer("", 0)
    , query_context(query_context_)
{
}

void ExternalStreamSource::getPhysicalHeader()
{
    auto non_virtual_header = storage_snapshot->metadata->getSampleBlockNonMaterialized();
    for (const auto & col : header)
    {
        /// The _tp_message_key column always maps to the message key.
        /// The _tp_time column always maps to the message timestamp.
        /// The _tp_message_headers column always maps to the message properties.
        if (col.name == ProtonConsts::RESERVED_MESSAGE_KEY || col.name == ProtonConsts::RESERVED_EVENT_TIME || col.name == ProtonConsts::RESERVED_MESSAGE_HEADERS)
            continue;

        if (std::ranges::any_of(non_virtual_header, [&col](auto & non_virtual_column) { return non_virtual_column.name == col.name; }))
            physical_header.insert(col);
    }

    /// Clients like to read virtual columns only, add the first physical column, then we know how many rows
    if (physical_header.columns() == 0)
    {
        const auto & physical_columns = storage_snapshot->getColumns(GetColumnsOptions::Ordinary);
        const auto & physical_column = physical_columns.front();
        physical_header.insert({physical_column.type->createColumn(), physical_column.type, physical_column.name});
    }
}

void ExternalStreamSource::initInputFormatExecutor(const String & data_format, const FormatSettings & format_settings)
{
    getPhysicalHeader();

    auto new_context = Context::createCopy(query_context);
    /// StreamingFormatExecutor cannot work with parallel parsing.
    new_context->setSetting("input_format_parallel_parsing", false);

    /// The buffer is only for initializing the input format, it won't be actually used, because the executor will keep setting new buffers.
    auto input_format
        = FormatFactory::instance().getInput(data_format, read_buffer, physical_header, new_context, max_block_size, format_settings);

    /// Formats of `RowInputFormatWithNamesAndTypes` (e.g. CSV, TSV, etc.) get the column list from either:
    /// 1) the first line of the data (i.e. the header), or
    /// 2) the passed-in Block, in this case, it's the `physical_header`.
    ///
    /// The issue is that, when the data do not have the header information (e.g. when the user use "CSV" instead of
    /// "CSVWithNames" or "CSVWithNamesAndTypes"), and the user only wants to SELECT a subset of the columns, i.e.
    /// `physical_header` has less columns than `storage_snapshot->metadata->getSampleBlock()`, then the format
    /// won't be able to parse the data properly and report error.
    ///
    /// Thus, for such case, we need to use the column_mapping to tell the format how to read the data properly.
    if (auto * input = dynamic_cast<RowInputFormatWithNamesAndTypes *>(input_format.get());
        input != nullptr && !(data_format.ends_with("WithNames") || data_format.ends_with("WithNamesAndTypes")))
    {
        auto column_mapping = input->getColumnMapping();
        auto schema = storage_snapshot->metadata->getSampleBlock();
        if (auto pos = schema.tryGetPositionByName(ProtonConsts::RESERVED_EVENT_TIME); pos)
            schema.erase(*pos);
        if (auto pos = schema.tryGetPositionByName(ProtonConsts::RESERVED_MESSAGE_KEY); pos)
            schema.erase(*pos);
        if (auto pos = schema.tryGetPositionByName(ProtonConsts::RESERVED_MESSAGE_HEADERS); pos)
            schema.erase(*pos);

        column_mapping->is_set = true;
        column_mapping->column_indexes_for_input_fields.resize(schema.columns());
        column_mapping->names_of_columns = schema.getNames();

        for (size_t i = 0; const auto & name : column_mapping->names_of_columns)
        {
            if (auto pos = physical_header.tryGetPositionByName(name))
                column_mapping->column_indexes_for_input_fields[i] = pos;

            ++i;
        }
    }

    format_executor = std::make_unique<StreamingFormatExecutor>(
        physical_header, std::move(input_format), [](const MutableColumns &, Exception & ex) -> size_t { throw std::move(ex); });
}

}
