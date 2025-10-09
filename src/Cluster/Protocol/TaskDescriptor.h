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
SERDE struct TaskDescriptor
{
public:
    TaskDescriptor() = default;

    TaskDescriptor(const TaskDescriptor &) = default;
    TaskDescriptor(TaskDescriptor &&) = default;

    TaskDescriptor(const DB::UUID & id_, std::string ns_, std::string name_, std::string sql_, uint32_t data_version_)
        : data_version(data_version_), id(id_), ns(std::move(ns_)), name(std::move(name_)), sql(std::move(sql_))
    {
    }

    void serialize(DB::WriteBuffer & wb, uint16_t version_) const;
    void deserialize(DB::ReadBuffer & rb, uint16_t version_);

    [[nodiscard]] size_t approximateSerializedSize() const noexcept;

    [[nodiscard]] std::string string() const;

    [[nodiscard]] bool isValid() const noexcept;

    /// `schema_version` is version used in serde. Bump up it when the on-disk schema is changed.
    constexpr static uint32_t schema_version = 1;
    /// `data_version` is used guardrail / version check on update.
    /// Whenever we change the data, we bump up the version.
    uint32_t data_version = Nulls::NullVersion;

    DB::UUID id{DB::UUIDHelpers::Nil};
    std::string ns;
    std::string name;

    int64_t create_timestamp_ms{0};
    int64_t last_modify_timestamp_ms{0};

    std::string created_by;
    std::string last_modified_by;

    std::string cron;
    uint64_t interval;
    DB::IntervalKind interval_unit;

    uint64_t timeout;
    DB::IntervalKind timeout_unit;

    std::vector<std::string> checkpoint_columns;
    std::string checkpoint_init_values;

    std::string target_database_name;
    std::string target_table_name;

    std::string sql;

    uint32_t status{0}; /// 0 - enabled; 1 - disabled
};

using TaskDescriptorPtr = std::shared_ptr<TaskDescriptor>;
using TaskDescriptorPtrs = std::vector<TaskDescriptorPtr>;
using TaskDescriptors = std::vector<TaskDescriptor>;
}
