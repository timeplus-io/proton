SET query_mode = 'table';
SET asterisk_include_reserved_columns=false;

DROP STREAM IF EXISTS replace;

create stream replace ( EventDate date,  Id uint64,  Data string,  Version uint32) ENGINE = ReplacingMergeTree(EventDate, Id, 8192, Version);
INSERT INTO replace VALUES ('2016-06-02', 1, 'version 1', 1);
INSERT INTO replace VALUES ('2016-06-02', 2, 'version 1', 1);
INSERT INTO replace VALUES ('2016-06-02', 1, 'version 0', 0);
SELECT sleep(3);

SELECT * FROM replace ORDER BY Id, Version;
SELECT * FROM replace FINAL ORDER BY Id, Version;
SELECT * FROM replace FINAL WHERE Version = 0 ORDER BY Id, Version;

DROP STREAM replace;
