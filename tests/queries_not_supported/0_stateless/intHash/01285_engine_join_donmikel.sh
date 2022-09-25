#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --multiquery --query "
DROP STREAM IF EXISTS NmSubj;
DROP STREAM IF EXISTS events;

create stream NmSubj
(
    NmId      uint32,
    SubjectId uint32
)
    engine = Join(All, inner, NmId);

create stream events
(
    EventDate       date,
    EventDateTime   datetime,
    EventId         string,
    SessionId       FixedString(36),
    PageViewId      FixedString(36),
    UserId          uint64,
    UniqUserId      FixedString(36),
    UrlReferrer     string,
    Param1          string,
    Param2          string,
    Param3          string,
    Param4          string,
    Param5          string,
    Param6          string,
    Param7          string,
    Param8          string,
    Param9          string,
    Param10         string,
    ApplicationType uint8,
    Locale          string,
    Lang            string,
    Version         string,
    Path            string,
    QueryString     string,
    UserHostAddress uint32
)
    engine = MergeTree()
        PARTITION BY (toYYYYMM(EventDate), EventId)
        ORDER BY (EventId, EventDate, Locale, ApplicationType, intHash64(UserId))
        SAMPLE BY intHash64(UserId)
        SETTINGS index_granularity = 8192;

insert into NmSubj values (1, 1), (2, 2), (3, 3);
"

$CLICKHOUSE_CLIENT --query "INSERT INTO events FORMAT TSV" < "${CURDIR}"/01285_engine_join_donmikel.tsv

$CLICKHOUSE_CLIENT --query "
SELECT to_int32(count() / 24) as Count
FROM events as e INNER JOIN NmSubj as ns
ON ns.NmId = to_uint32(e.Param1)
WHERE e.EventDate = today() - 7 AND e.EventId = 'GCO' AND ns.SubjectId = 2073"

$CLICKHOUSE_CLIENT --multiquery --query "
DROP STREAM NmSubj;
DROP STREAM events;
"
