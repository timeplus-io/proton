#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
SCHEMADIR=$CURDIR/format_schemas
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -eo pipefail

# Run the client.
$CLICKHOUSE_CLIENT --multiquery <<EOF
DROP STREAM IF EXISTS squares_protobuf_00825;

create stream squares_protobuf_00825 (number uint32, square uint64) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO squares_protobuf_00825 VALUES (2, 4), (0, 0), (3, 9);

SELECT * FROM squares_protobuf_00825;
EOF

BINARY_FILE_PATH=$(mktemp "$CURDIR/00825_protobuf_format_nested_optional.XXXXXX.binary")
$CLICKHOUSE_CLIENT --query "SELECT * FROM squares_protobuf_00825 FORMAT Protobuf SETTINGS format_schema = '$SCHEMADIR/00825_protobuf_format_squares:NumberAndSquare'" > "$BINARY_FILE_PATH"

# Check the output in the protobuf format
echo
$CURDIR/helpers/protobuf_length_delimited_encoder.py --decode_and_check --format_schema "$SCHEMADIR/00825_protobuf_format_squares:NumberAndSquare" --input "$BINARY_FILE_PATH"

# Check the input in the protobuf format (now the table contains the same data twice).
echo
$CLICKHOUSE_CLIENT --query "INSERT INTO squares_protobuf_00825 FORMAT Protobuf SETTINGS format_schema='$SCHEMADIR/00825_protobuf_format_squares:NumberAndSquare'" < "$BINARY_FILE_PATH"
$CLICKHOUSE_CLIENT --query "SELECT * FROM squares_protobuf_00825"

rm "$BINARY_FILE_PATH"
$CLICKHOUSE_CLIENT --query "DROP STREAM squares_protobuf_00825"
