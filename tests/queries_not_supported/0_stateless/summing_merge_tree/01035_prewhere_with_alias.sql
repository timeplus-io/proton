-- Tags: not_supported, blocked_by_SummingMergeTree
SET query_mode = 'table';
DROP STREAM IF EXISTS test;
create stream test (a uint8, b uint8, c uint16 ALIAS a + b) ENGINE = MergeTree ORDER BY a;

SELECT b FROM test PREWHERE c = 1;

DROP STREAM test;

drop stream if exists audience_local;
create stream audience_local
(
 date date,
 AudienceType Enum8('other' = 0, 'client' = 1, 'group' = 2),
 UMA uint64,
 APIKey string,
 TrialNameID uint32,
 TrialGroupID uint32,
 AppVersion string,
 Arch Enum8('other' = 0, 'x32' = 1, 'x64' = 2),
 UserID uint32,
 GroupID uint8,
 OSName Enum8('other' = 0, 'Android' = 1, 'iOS' = 2, 'macOS' = 3, 'Windows' = 4, 'Linux' = 5),
 Channel Enum8('other' = 0, 'Canary' = 1, 'Dev' = 2, 'Beta' = 3, 'Stable' = 4),
 Hits uint64,
 Sum int64,
 Release string alias splitByChar('-', AppVersion)[1]
)
engine = SummingMergeTree
PARTITION BY (toISOYear(date), toISOWeek(date))
ORDER BY (AudienceType, UMA, APIKey, date, TrialNameID, TrialGroupID, AppVersion, Arch, UserID, GroupID, OSName, Channel)
SETTINGS index_granularity = 8192;

SELECT DISTINCT UserID
FROM audience_local
PREWHERE date = to_date('2019-07-25') AND Release = '17.11.0.542';

drop stream if exists audience_local;
