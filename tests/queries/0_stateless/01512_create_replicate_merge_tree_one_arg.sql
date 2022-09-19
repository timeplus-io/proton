-- Tags: replica

create stream mt (v uint8) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_01497/mt')
    ORDER BY tuple() -- { serverError 36 }

