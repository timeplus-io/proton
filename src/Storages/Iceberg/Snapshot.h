#include "config.h"

#if USE_AVRO

#include <string>

/// Represents a snapshot in the Iceberg table format.
struct Snapshot {
    /// A unique long ID for the snapshot.
    int64_t snapshot_id;

    /// The snapshot ID of the snapshot's parent.
    /// This field is omitted for snapshots with no parent.
    int64_t parent_snapshot_id;

    /// A monotonically increasing long that tracks the order of changes to a table.
    int64_t sequence_number;

    /// A timestamp in milliseconds when the snapshot was created.
    /// Used for garbage collection and table inspection.
    int64_t timestamp_ms;

    /// The location of a manifest list for this snapshot
    /// that tracks manifest files with additional metadata.
    std::string manifest_list;

    /// A map summarizing the snapshot changes.
    /// The "operation" field is required and describes the type of operation.
    std::unordered_map<std::string, std::string> summary;

    /// ID of the table's current schema when the snapshot was created.
    int schema_id;

    /// The first `_row_id` assigned to the first row in the first data file in the first manifest.
    int64_t first_row_id;

    /// Sum of the `added_rows_count` from all manifests added in this snapshot.
    int64_t added_rows;
};
#endif
