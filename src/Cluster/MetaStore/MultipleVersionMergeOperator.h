#pragma once

#include <Cluster/Common/EncodeMetaKey.h>
#include <IO/ReadBufferFromString.h>

#include <rocksdb/merge_operator.h>

namespace Poco
{
class Logger;
}

namespace cluster::meta
{
/// MultipleVersionEncoder encodes multiple versions of metadata in a compact way
/// MultiVersionMarker (8bit) | Version (8bit) | Versions (8bit) | MetaObject v1 | MetaObject v2 | ...
/// MultiVersionMarker - indicates that the current value encoding is multiple version
/// Version - tells the version of this encoding (could change in future for migration, backward compatibility)
/// Versions - Number of versions of the metadata encoded in the value. We only supports 256 versions for now
/// MetaObject v1, MetaObject v2, ... - Serialized meta objects in a consecutive way
struct MultipleVersionEncoder
{
    static void encodeHeader(uint8_t number_versions, std::string & value);

    static void encodeHeader(DB::WriteBuffer & wb, uint8_t number_versions);

    static void decodeHeader(std::string_view value, uint8_t & number_versions);

    static void decodeHeader(DB::ReadBuffer & rb, uint8_t & number_versions);

    /// It is still possible but not likely an object's first 3 bytes are encoded exactly same
    /// as multi-version header, so we will need pay attention the decoding error
    static bool likelyMultiVersionEncoded(std::string_view value) noexcept
    {
        /// When VERSION is changed, we will need revise static_cast<uint8_t>(value[1] == VERSION)
        return value.size() > headerSize() && static_cast<uint8_t>(value[0]) == MULTI_VERSION_MARKER
            && static_cast<uint8_t>(value[1] == VERSION) && static_cast<uint8_t>(value[2]) <= MAX_VERSIONS;
    }

    template <typename T>
    static std::shared_ptr<T> decodeLatest(MetaKeySpace key_space, std::string_view key, std::string_view value, uint32_t version)
    {
        auto results = decode<T>(key_space, key, value, /*versions_requested=*/1, version);
        return std::move(results[0]);
    }

    template <typename T>
    static std::vector<std::shared_ptr<T>> decode(
        [[maybe_unused]] MetaKeySpace key_space,
        [[maybe_unused]] std::string_view key,
        std::string_view value,
        uint8_t versions_requested,
        uint32_t version)
    {
        assert(!key.empty());

        auto decode_single_version = [&]() {
            DB::ReadBufferFromString rb(value);
            auto t = std::make_shared<T>();
            t->deserialize(rb, version);
            return std::vector{std::move(t)};
        };

        if (likelyMultiVersionEncoded(value))
        {
            assert(key_space == decodeMetaKeySpace(key));

            DB::ReadBufferFromString rb(value);
            uint8_t number_versions = 0;
            decodeHeader(rb, number_versions);

            uint8_t versions_to_skip = versions_requested >= number_versions ? 0 : number_versions - versions_requested;

            uint8_t i = 0;
            try
            {
                for (; i < versions_to_skip; ++i)
                {
                    T t;
                    t.deserialize(rb, version);
                }

                std::vector<std::shared_ptr<T>> results;
                results.reserve(number_versions - versions_to_skip);

                for (; i < number_versions; ++i)
                {
                    results.push_back(std::make_shared<T>());
                    results.back()->deserialize(rb, version);
                }
                return results;
            }
            catch (...)
            {
                return decode_single_version();
            }
        }
        else
        {
            return decode_single_version();
        }
    }

    static constexpr size_t headerSize() noexcept { return sizeof(MULTI_VERSION_MARKER) + sizeof(VERSION) + /*Versions*/ 1; }

    static constexpr uint8_t MULTI_VERSION_MARKER = 0xff;

    static constexpr uint8_t MAX_VERSIONS = 0x7f;

    /// Current header version
    /// Note when VERSION upgrades, remember to check everywhere which refs it to see if corresponding
    /// updates are required. One place will be `multiVersionEncoded` for example
    static constexpr uint8_t VERSION = 0x1;
};

/// Multiple version merge operator keeps the latest N versions of data when merging.
class MultipleVersionMergeOperator final : public rocksdb::MergeOperator
{
public:
    MultipleVersionMergeOperator(uint8_t keep_version_, LoggerPtr logger_) : logger(logger_)
    {
        keep_versions = std::min(keep_version_, MultipleVersionEncoder::MAX_VERSIONS);
    }

    static const char * Type() { return "MultipleVersionMergeOperator"; }

    const char * Name() const override { return Type(); }

    bool FullMergeV2(const MergeOperationInput & merge_in, MergeOperationOutput * merge_out) const override;

private:
    bool truncateVersions(MetaKeySpace key_space, DB::ReadBuffer & rb, rocksdb::Slice & value, size_t versions_to_truncate) const;
    bool doFullMergeV2(const MergeOperationInput & merge_in, MergeOperationOutput * merge_out, bool skip_existing_value = false) const;
    bool recoverMerge(const MergeOperationInput & merge_in, MergeOperationOutput * merge_out) const;
    bool validateMerged(MetaKeySpace meta_key_space, rocksdb::Slice merged) const;

private:
    uint8_t keep_versions;
    LoggerPtr logger;
};

}
