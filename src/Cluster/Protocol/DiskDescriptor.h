#pragma once

#include <Cluster/Common/Common.h>


namespace DB
{
class WriteBuffer;
class ReadBuffer;
}

namespace cluster::protocol
{
struct DiskDescriptor
{
    size_t approximateSerializedSize() const noexcept;

    void serialize(DB::WriteBuffer & wb, uint16_t version_) const;
    void deserialize(DB::ReadBuffer & rb, uint16_t version_);

    std::string string() const;

    /// `schema_version` is version used in serde. Bump up it when the on-disk schema is changed.
    constexpr static uint32_t schema_version = 1;

    /// `data_version` is used guardrail / version check on update.
    /// Whenever we change the data, we bump up the version.
    uint32_t data_version = Nulls::NullVersion;

    enum class DiskType : uint8_t
    {
        Unknown = 0,
        Local = 1,
        Encrypted = 2,
        Cache = 3,
        S3 = 4,
        S3Plain = 5,
        Web = 6,
        AzureBlobStorage = 7,
        LocalBlobStorage = 8,
    };

    inline static DiskType parseDiskType(const std::string & type)
    {
        if (type == "local")
            return DiskType::Local;
        else if (type == "encrypted")
            return DiskType::Encrypted;
        else if (type == "cache")
            return DiskType::Cache;
        else if (type == "s3")
            return DiskType::S3;
        else if (type == "s3_plain")
            return DiskType::S3Plain;
        else if (type == "web")
            return DiskType::Web;
        else if (type == "azure_blob_storage")
            return DiskType::AzureBlobStorage;
        else if (type == "local_blob_storage")
            return DiskType::LocalBlobStorage;
        else
            return DiskType::Unknown;
    }

    DiskType type;
    std::string name;
    std::string config;

    int64_t create_timestamp_ms;
    int64_t last_modify_timestamp_ms;

    std::string created_by;
    std::string last_modified_by;
};

using DiskDescriptorPtr = std::shared_ptr<DiskDescriptor>;
using DiskDescriptorPtrs = std::vector<DiskDescriptorPtr>;
}
