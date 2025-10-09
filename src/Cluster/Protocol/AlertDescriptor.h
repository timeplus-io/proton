#pragma once

#include <Cluster/Common/Common.h>
#include <Cluster/Common/SerdeTag.h>
#include <Core/UUID.h>
#include <Common/IntervalKind.h>

namespace DB
{
class WriteBuffer;
class ReadBuffer;
}

namespace cluster::protocol
{
SERDE struct AlertDescriptor
{
    SERDE struct PeriodicCount
    {
    public:
        void serialize(DB::WriteBuffer & wb, uint16_t version_) const;
        void deserialize(DB::ReadBuffer & rb, uint16_t version_);

        bool operator==(const PeriodicCount & rhs) const;
        [[nodiscard]] std::string string() const;
        [[nodiscard]] size_t approximateSerializedSize() const noexcept;

    public:
        uint64_t count{0};
        uint64_t interval{0};
        DB::IntervalKind interval_kind;
    };

public:
    AlertDescriptor() = default;

    AlertDescriptor(const AlertDescriptor &) = default;
    AlertDescriptor(AlertDescriptor &&) = default;

    AlertDescriptor(
        const DB::UUID & id_,
        const std::string & ns_,
        const std::string & name_,
        const std::string & function_name_,
        const std::string & select_query_,
        const PeriodicCount & batch_,
        const PeriodicCount & limit_,
        uint32_t data_version_)
        : data_version(data_version_)
        , id(id_)
        , ns(ns_)
        , name(name_)
        , function_name(function_name_)
        , select_query(select_query_)
        , batch(batch_)
        , limit(limit_)
    {
    }

    AlertDescriptor(
        DB::UUID && id_,
        std::string && ns_,
        std::string && name_,
        std::string && function_name_,
        std::string && select_query_,
        PeriodicCount && batch_,
        PeriodicCount && limit_,
        uint32_t data_version_)
        : data_version(data_version_)
        , id(id_)
        , ns(std::move(ns_))
        , name(std::move(name_))
        , function_name(std::move(function_name_))
        , select_query(std::move(select_query_))
        , batch(std::move(batch_))
        , limit(std::move(limit_))
    {
    }

    void serialize(DB::WriteBuffer & wb, uint16_t version_) const;
    void deserialize(DB::ReadBuffer & rb, uint16_t version_);

    [[nodiscard]] size_t approximateSerializedSize() const noexcept;

    bool operator==(const AlertDescriptor & rhs) const;

    [[nodiscard]] std::string string() const;

    [[nodiscard]] bool isValid() const noexcept;

public:
    /// `schema_version` is version used in serde. Bump up it when the on-disk schema is changed.
    constexpr static uint32_t schema_version = 1;
    /// `data_version` is used guardrail / version check on update.
    /// Whenever we change the data, we bump up the version.
    uint32_t data_version = Nulls::NullVersion;

    DB::UUID id{DB::UUIDHelpers::Nil};
    std::string ns;
    std::string name;
    std::string function_name;
    std::string select_query;

    PeriodicCount batch;
    PeriodicCount limit;

    int64_t create_timestamp_ms{0};
    int64_t last_modify_timestamp_ms{0};

    std::string created_by;
    std::string last_modified_by;
};

using AlertDescriptorPtr = std::shared_ptr<AlertDescriptor>;
using AlertDescriptorPtrs = std::vector<AlertDescriptorPtr>;
using AlertDescriptors = std::vector<AlertDescriptor>;
}
