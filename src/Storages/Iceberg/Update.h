#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>

namespace Poco::JSON
{
class Array;
}

namespace Apache::Iceberg
{

class Update
{
public:
    explicit Update(const std::string & action_) : action(action_) { }
    virtual ~Update() = default;

    virtual void apply(Poco::JSON::Array & updates) const = 0;

    const std::string & getAction() const { return action; }

protected:
    const std::string action;
};

struct Snapshot
{
    /// Only `operation` is required.
    struct Summary
    {
        std::string operation;
        std::string added_files_size;
        std::string added_data_files;
        std::string added_records;
        std::string total_data_files;
        std::string total_delete_files;
        std::string total_records;
        std::string total_files_size;
        std::string total_position_deletes;
        std::string total_equality_deletes;
        /// There are even more fields not listed here
        /// TODO at least should add engine-name and engine-version
    };

    int64_t snapshot_id{-1};
    std::optional<int64_t> parent_snapshot_id;
    uint64_t sequence_number{0};
    int64_t timestamp_ms{0};
    std::string manifest_list;
    Summary summary;

    bool isValid() const { return snapshot_id > 0; }
};

class AddSnopshotUpdate : public Update
{
public:
    explicit AddSnopshotUpdate(const Snapshot & snapshot_) : Update("add-snapshot"), snapshot(snapshot_) { }
    ~AddSnopshotUpdate() override = default;

    void apply(Poco::JSON::Array & updates) const override;

private:
    Snapshot snapshot;
};

struct SnapshotReference
{
    std::string name;
    std::string type;
    int64_t snapshot_id;
};

class SetSnapshotRefUpdate : public Update
{
public:
    explicit SetSnapshotRefUpdate(const SnapshotReference & snapshot_ref_) : Update("set-snapshot-ref"), snapshot_ref(snapshot_ref_) { }
    ~SetSnapshotRefUpdate() override = default;

    void apply(Poco::JSON::Array & updates) const override;

private:
    SnapshotReference snapshot_ref;
};

using UpdatePtr = std::unique_ptr<Update>;
using Updates = std::vector<UpdatePtr>;

}
