#!/usr/bin/env bash
# Tags: no-fasttest

# https://github.com/ClickHouse/ClickHouse/issues/31160

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCHEMADIR=$CURDIR/format_schemas
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -eo pipefail

# Run the client.
$CLICKHOUSE_CLIENT --multiquery <<EOF
DROP STREAM IF EXISTS table_skipped_column_in_nested_00825;

create stream table_skipped_column_in_nested_00825 (
    identifier UUID,
    unused1 string,
    modules nested (
        module_id uint32,
        supply uint32,
        temp uint32
    ),
    modules_nodes nested (
        opening_time array(uint32),
        node_id array(uint32),
        closing_time_time array(uint32),
        current array(uint32),
        coords nested (
            x Float32,
            y float64
        )
    )
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO table_skipped_column_in_nested_00825 VALUES ('e4048ead-30a2-45e5-90be-2af1c7137523', 'dummy', [1], [50639], [58114], [[5393]], [[1]], [[3411]], [[17811]], [[(10, 20)]]);

SELECT * FROM table_skipped_column_in_nested_00825;
EOF

BINARY_FILE_PATH=$(mktemp "$CURDIR/00825_protobuf_format_skipped_column_in_nested.XXXXXX.binary")
$CLICKHOUSE_CLIENT --query "SELECT * FROM table_skipped_column_in_nested_00825 FORMAT Protobuf SETTINGS format_schema = '$SCHEMADIR/00825_protobuf_format_skipped_column_in_nested:UpdateMessage'" > "$BINARY_FILE_PATH"

# Check the output in the protobuf format
echo
$CURDIR/helpers/protobuf_length_delimited_encoder.py --decode_and_check --format_schema "$SCHEMADIR/00825_protobuf_format_skipped_column_in_nested:UpdateMessage" --input "$BINARY_FILE_PATH"

# Check the input in the protobuf format (now the table contains the same data twice).
echo
$CLICKHOUSE_CLIENT --query "INSERT INTO table_skipped_column_in_nested_00825 FORMAT Protobuf SETTINGS format_schema='$SCHEMADIR/00825_protobuf_format_skipped_column_in_nested:UpdateMessage'" < "$BINARY_FILE_PATH"
$CLICKHOUSE_CLIENT --query "SELECT * FROM table_skipped_column_in_nested_00825 ORDER BY unused1"

rm "$BINARY_FILE_PATH"
$CLICKHOUSE_CLIENT --query "DROP STREAM table_skipped_column_in_nested_00825"
