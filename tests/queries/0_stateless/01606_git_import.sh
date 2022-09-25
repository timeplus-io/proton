#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Clone some not too large repository and create a database from it.

cd $CLICKHOUSE_TMP || exit

# Protection for network errors
for _ in {1..10}; do
    rm -rf ./clickhouse-odbc
    git clone --quiet https://github.com/ClickHouse/clickhouse-odbc.git && pushd clickhouse-odbc > /dev/null && git checkout --quiet 5d84ec591c53cbb272593f024230a052690fdf69 && break
    sleep 1
done

${CLICKHOUSE_GIT_IMPORT} 2>&1 | wc -l

${CLICKHOUSE_CLIENT} --multiline --multiquery --query "

DROP STREAM IF EXISTS commits;
DROP STREAM IF EXISTS file_changes;
DROP STREAM IF EXISTS line_changes;

create stream commits
(
    hash string,
    author low_cardinality(string),
    time datetime,
    message string,
    files_added uint32,
    files_deleted uint32,
    files_renamed uint32,
    files_modified uint32,
    lines_added uint32,
    lines_deleted uint32,
    hunks_added uint32,
    hunks_removed uint32,
    hunks_changed uint32
) ENGINE = MergeTree ORDER BY time;

create stream file_changes
(
    change_type Enum('Add' = 1, 'Delete' = 2, 'Modify' = 3, 'Rename' = 4, 'Copy' = 5, 'Type' = 6),
    path low_cardinality(string),
    old_path low_cardinality(string),
    file_extension low_cardinality(string),
    lines_added uint32,
    lines_deleted uint32,
    hunks_added uint32,
    hunks_removed uint32,
    hunks_changed uint32,

    commit_hash string,
    author low_cardinality(string),
    time datetime,
    commit_message string,
    commit_files_added uint32,
    commit_files_deleted uint32,
    commit_files_renamed uint32,
    commit_files_modified uint32,
    commit_lines_added uint32,
    commit_lines_deleted uint32,
    commit_hunks_added uint32,
    commit_hunks_removed uint32,
    commit_hunks_changed uint32
) ENGINE = MergeTree ORDER BY time;

create stream line_changes
(
    sign int8,
    line_number_old uint32,
    line_number_new uint32,
    hunk_num uint32,
    hunk_start_line_number_old uint32,
    hunk_start_line_number_new uint32,
    hunk_lines_added uint32,
    hunk_lines_deleted uint32,
    hunk_context low_cardinality(string),
    line low_cardinality(string),
    indent uint8,
    line_type Enum('Empty' = 0, 'Comment' = 1, 'Punct' = 2, 'Code' = 3),

    prev_commit_hash string,
    prev_author low_cardinality(string),
    prev_time datetime,

    file_change_type Enum('Add' = 1, 'Delete' = 2, 'Modify' = 3, 'Rename' = 4, 'Copy' = 5, 'Type' = 6),
    path low_cardinality(string),
    old_path low_cardinality(string),
    file_extension low_cardinality(string),
    file_lines_added uint32,
    file_lines_deleted uint32,
    file_hunks_added uint32,
    file_hunks_removed uint32,
    file_hunks_changed uint32,

    commit_hash string,
    author low_cardinality(string),
    time datetime,
    commit_message string,
    commit_files_added uint32,
    commit_files_deleted uint32,
    commit_files_renamed uint32,
    commit_files_modified uint32,
    commit_lines_added uint32,
    commit_lines_deleted uint32,
    commit_hunks_added uint32,
    commit_hunks_removed uint32,
    commit_hunks_changed uint32
) ENGINE = MergeTree ORDER BY time;
"

${CLICKHOUSE_CLIENT} --query "INSERT INTO commits FORMAT TSV" < commits.tsv
${CLICKHOUSE_CLIENT} --query "INSERT INTO file_changes FORMAT TSV" < file_changes.tsv
${CLICKHOUSE_CLIENT} --query "INSERT INTO line_changes FORMAT TSV" < line_changes.tsv

${CLICKHOUSE_CLIENT} --query "SELECT count() FROM commits"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM file_changes"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM line_changes"

${CLICKHOUSE_CLIENT} --multiline --multiquery --query "
DROP STREAM commits;
DROP STREAM file_changes;
DROP STREAM line_changes;
"

