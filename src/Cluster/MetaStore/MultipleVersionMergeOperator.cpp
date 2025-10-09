#include <Cluster/Common/serde.h>
#include <Cluster/MetaStore/MultipleVersionMergeOperator.h>
#include <Cluster/Protocol/DatabaseDescriptor.h>
#include <Cluster/Protocol/FormatSchemaDescriptor.h>
#include <Cluster/Protocol/StreamDescriptor.h>
#include <Cluster/Protocol/UserDefinedFunctionDescriptor.h>

#include <Common/logger_useful.h>

namespace cluster::meta
{

void MultipleVersionEncoder::encodeHeader(uint8_t number_versions, std::string & value)
{
    DB::WriteBufferFromString wb(value);
    encodeHeader(wb, number_versions);
}

void MultipleVersionEncoder::encodeHeader(DB::WriteBuffer & wb, uint8_t number_versions)
{
    DB::writeBinary(MULTI_VERSION_MARKER, wb);
    DB::writeBinary(VERSION, wb);
    DB::writeBinary(number_versions, wb);
}

void MultipleVersionEncoder::decodeHeader(std::string_view value, uint8_t & number_versions)
{
    DB::ReadBufferFromString rb{value};
    decodeHeader(rb, number_versions);
}

void MultipleVersionEncoder::decodeHeader(DB::ReadBuffer & rb, uint8_t & number_versions)
{
    uint8_t marker = 0;
    DB::readBinary(marker, rb);
    assert(marker == MULTI_VERSION_MARKER);

    uint8_t version = 0;
    DB::readBinary(version, rb);
    assert(version == VERSION);

    DB::readBinary(number_versions, rb);
    chassert(number_versions <= MultipleVersionEncoder::MAX_VERSIONS);
}

bool MultipleVersionMergeOperator::FullMergeV2(
    const rocksdb::MergeOperator::MergeOperationInput & merge_in, rocksdb::MergeOperator::MergeOperationOutput * merge_out) const
{
    /// We can't let exception escape from full merge, it will abort the whole process
    try
    {
        return doFullMergeV2(merge_in, merge_out);
    }
    catch (const DB::Exception & ex)
    {
        LOG_ERROR(
            logger,
            "Failed to full merge key={} error_message={}, discard all old version and only keep the latest version",
            merge_in.key.ToStringView(),
            ex.message());

        return recoverMerge(merge_in, merge_out);
    }
    catch (...)
    {
        DB::tryLogCurrentException(
            logger,
            fmt::format(
                "Failed to full merge key={}, discard all old version and only keep the latest version", merge_in.key.ToStringView()));

        return recoverMerge(merge_in, merge_out);
    }
}

bool MultipleVersionMergeOperator::recoverMerge(
    const rocksdb::MergeOperator::MergeOperationInput & merge_in, rocksdb::MergeOperator::MergeOperationOutput * merge_out) const
{
    /// When we reach here is it is likely data is already corrupted in existing value
    try
    {
        doFullMergeV2(merge_in, merge_out, /*skip_existing_value=*/true);
    }
    catch (...)
    {
        LOG_ERROR(logger, "Recover merge by skipping existing value still failed, using last version in the operand list");
        if (!merge_in.operand_list.empty())
            merge_out->existing_operand = merge_in.operand_list.back();
    }
    return true;
}

/// \return true if it is a multi-version encoded object
bool MultipleVersionMergeOperator::validateMerged(MetaKeySpace meta_key_space, rocksdb::Slice merged) const
{
    DB::ReadBufferFromString rb(merged.ToStringView());
    uint8_t versions = 0;
    MultipleVersionEncoder::decodeHeader(rb, versions);

    LOG_INFO(
        logger,
        "Found merged operand for key_space=0x{:x} which is unlikely, so validating the data by deserializing all "
        "versions={} objects",
        std::to_underlying(meta_key_space),
        versions);

    try
    {
        truncateVersions(meta_key_space, rb, merged, versions);
    }
    catch (...)
    {
        LOG_INFO(
            logger,
            "Failed to validate merged operand for key_space=0x{:x}, will treat it as single version",
            std::to_underlying(meta_key_space));
        return false;
    }
    return true;
}

/// There may be several cases
/// [Put/Delete, Merge, Merge, Merge, ...]
/// [Merge, Merge, Merge, Merge, ...]
/// [Merged, Merged, Merged, Merge, Merge, Merge, ...]
///
/// There shall have no such case : Merge and Merged interleaving ?
/// [Put, Merge, Merged, Merge, Merge, Merged, Merge, ...]
///
bool MultipleVersionMergeOperator::doFullMergeV2(
    const rocksdb::MergeOperator::MergeOperationInput & merge_in,
    rocksdb::MergeOperator::MergeOperationOutput * merge_out,
    bool skip_existing_value) const
{
    size_t total_versions = 0;

    /// Copy the operands
    std::vector<rocksdb::Slice> operand_list;
    operand_list.reserve(merge_in.operand_list.size());
    size_t operand_bytes = 0;

    auto meta_key_space = decodeMetaKeySpace(merge_in.key.ToStringView());
    {
        auto riter = merge_in.operand_list.rbegin();
        auto rend = merge_in.operand_list.rend();

        for (; riter != rend; ++riter)
        {
            if (!MultipleVersionEncoder::likelyMultiVersionEncoded(riter->ToStringView()))
            {
                ++total_versions;
                operand_list.push_back(*riter);
                operand_bytes += riter->size();

                if (total_versions == keep_versions)
                    break;
            }
            else
            {
                if (!validateMerged(meta_key_space, *riter))
                {
                    /// Treat it as single version encoded value
                    ++total_versions;
                    operand_list.push_back(*riter);
                    operand_bytes += riter->size();

                    if (total_versions == keep_versions)
                        break;
                    else
                        continue;
                }

                auto value_copy = *riter;
                DB::ReadBufferFromString rb(value_copy.ToStringView());

                uint8_t versions = 0;
                MultipleVersionEncoder::decodeHeader(rb, versions);
                total_versions += versions;

                if (total_versions < keep_versions)
                {
                    value_copy.remove_prefix(MultipleVersionEncoder::headerSize());
                    operand_list.push_back(value_copy);
                    operand_bytes += value_copy.size();
                }
                else if (total_versions == keep_versions)
                {
                    value_copy.remove_prefix(MultipleVersionEncoder::headerSize());
                    operand_list.push_back(value_copy);
                    operand_bytes += value_copy.size();
                    break;
                }
                else
                {
                    LOG_INFO(
                        logger,
                        "Truncating version because of max version exceeded by merged operand, existing value has merged versions={} "
                        "total_versions={} keep_versions={} for key_space=0x{:x} key={}",
                        versions,
                        total_versions,
                        keep_versions,
                        std::to_underlying(meta_key_space),
                        merge_in.key.ToStringView());

                    if (truncateVersions(meta_key_space, rb, value_copy, total_versions - keep_versions))
                    {
                        operand_list.push_back(value_copy);
                        operand_bytes += value_copy.size();
                        total_versions = keep_versions;
                    }
                    else
                    {
                        /// If truncation is not handled, we will need remove the merged versions
                        total_versions -= versions;
                    }

                    break;
                }
            }
        }
    }

    /// `init_value` already got its header stripped
    auto do_merge = [&](std::string_view init_value, uint8_t number_versions) {
        auto iter = operand_list.rbegin();
        auto iter_end = operand_list.rend();

        size_t total_size = MultipleVersionEncoder::headerSize() + operand_bytes;

        std::string merged;
        merged.reserve(total_size + init_value.size() + 16);

        /// Head
        MultipleVersionEncoder::encodeHeader(number_versions, merged);

        /// Initial value
        merged += init_value;

        /// Concatenate operand values
        /// We assume changes are full replace instead of incremental updates
        for (; iter != iter_end; ++iter)
            merged += iter->ToStringView();

        assert(merged.size() < total_size + init_value.size() + 16);

        merge_out->new_value.swap(merged);
    };

    if (merge_in.existing_value && !skip_existing_value)
    {
        if (total_versions >= keep_versions)
        {
            /// We don't even care the existing version in the existing value since there are more than keep_versions of new updates
            LOG_INFO(
                logger,
                "Skipping versions in existing value since the versions={} in the operand list already exceeds the max keep_versions={}",
                merge_in.operand_list.size(),
                keep_versions);

            do_merge(std::string_view{}, keep_versions);
        }
        else
        {
            auto value_copy = *merge_in.existing_value;
            DB::ReadBufferFromString rb(value_copy.ToStringView());

            /// Here it is still possible there is encoding coincidence, if it is, it will trigger
            /// recoverMerge for final recovery
            bool multi_version_encoded = MultipleVersionEncoder::likelyMultiVersionEncoded(value_copy.ToStringView());
            uint8_t versions = 1; /// Init to 1 for single version encoded value
            if (multi_version_encoded)
                MultipleVersionEncoder::decodeHeader(rb, versions);

            total_versions += versions;

            if (total_versions > keep_versions)
            {
                LOG_INFO(
                    logger,
                    "Truncating version because of max version exceeded, existing value has merged versions={} total_versions={} "
                    "keep_versions={} for key_space=0x{:x} key={}",
                    versions,
                    total_versions,
                    keep_versions,
                    std::to_underlying(meta_key_space),
                    merge_in.key.ToStringView());

                if (!truncateVersions(meta_key_space, rb, value_copy, total_versions - keep_versions) && multi_version_encoded)
                {
                    /// If truncation is not handled, we will need remove the merged versions
                    value_copy.clear();
                    total_versions -= versions;
                }
            }
            else if (multi_version_encoded)
            {
                value_copy.remove_prefix(MultipleVersionEncoder::headerSize());
            }

            do_merge(value_copy.ToStringView(), std::min<uint8_t>(total_versions, keep_versions));
        }
    }
    else
    {
        LOG_DEBUG(
            logger,
            "Merge total_versions={} in operand_list_size={} keep_versions={} skip_existing_value={}",
            total_versions,
            merge_in.operand_list.size(),
            keep_versions,
            skip_existing_value);

        do_merge(std::string_view{}, std::min<uint8_t>(total_versions, keep_versions));
    }

    return true;
}

/// \return true if value gets truncated, otherwise false
bool MultipleVersionMergeOperator::truncateVersions(
    MetaKeySpace key_space, DB::ReadBuffer & rb, rocksdb::Slice & value, size_t versions_to_truncate) const
{
    assert(versions_to_truncate != 0);

    size_t n = 0;
    while (n++ < versions_to_truncate)
    {
        switch (key_space)
        {
            case MetaKeySpace::Stream:
            {
                protocol::StreamDescriptor desc;
                desc.deserialize(rb, cluster::Nulls::NullVersion);
                break;
            }
            case MetaKeySpace::UDF:
            {
                protocol::UserDefinedFunctionDescriptor desc;
                desc.deserialize(rb, cluster::Nulls::NullVersion);
                break;
            }
            case MetaKeySpace::FormatSchema:
            {
                protocol::FormatSchemaDescriptor desc;
                desc.deserialize(rb, cluster::Nulls::NullVersion);
                break;
            }
            case MetaKeySpace::Database:
            {
                protocol::DatabaseDescriptor desc;
                desc.deserialize(rb, cluster::Nulls::NullVersion);
                break;
            }
            default:
            {
                LOG_ERROR(logger, "Unhandled key_space=0x{:x} for versioned truncation", std::to_underlying(key_space));
                return false;
            }
        }
    }

    assert(rb.offset() <= value.size());
    value.remove_prefix(rb.offset());
    return true;
}


}
