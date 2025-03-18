#include <Storages/Iceberg/Update.h>

#include <Poco/JSON/Object.h>

namespace Apache::Iceberg
{

void AddSnopshotUpdate::apply(Poco::JSON::Array & updates) const
{
    Poco::JSON::Object summary_obj;
    summary_obj.set("operation", snapshot.summary.operation);
    summary_obj.set("added-files-size", snapshot.summary.added_files_size);
    summary_obj.set("added-data-files", snapshot.summary.added_data_files);
    summary_obj.set("added-records", snapshot.summary.added_records);
    summary_obj.set("total-data-files", snapshot.summary.total_data_files);
    summary_obj.set("total-delete-files", snapshot.summary.total_delete_files);
    summary_obj.set("total-records", snapshot.summary.total_records);
    summary_obj.set("total-files-size", snapshot.summary.total_files_size);
    summary_obj.set("total-position-deletes", snapshot.summary.total_position_deletes);
    summary_obj.set("total-equality-deletes", snapshot.summary.total_equality_deletes);

    Poco::JSON::Object snapshot_obj;
    snapshot_obj.set("snapshot-id", snapshot.snapshot_id);
    if (snapshot.parent_snapshot_id)
        snapshot_obj.set("parent-snapshot-id", *snapshot.parent_snapshot_id);
    snapshot_obj.set("sequence-number", snapshot.sequence_number);
    snapshot_obj.set("timestamp-ms", snapshot.timestamp_ms);
    snapshot_obj.set("manifest-list", snapshot.manifest_list);
    snapshot_obj.set("summary", summary_obj);

    Poco::JSON::Object obj;
    obj.set("action", action);
    obj.set("snapshot", snapshot_obj);
    updates.add(obj);
}

void SetSnapshotRefUpdate::apply(Poco::JSON::Array & updates) const
{
    Poco::JSON::Object obj;
    obj.set("action", action);
    obj.set("ref-name", snapshot_ref.name);
    obj.set("type", snapshot_ref.type);
    obj.set("snapshot-id", snapshot_ref.snapshot_id);
    updates.add(obj);
}

}
