DROP STREAM IF EXISTS sites;
create stream sites (Domain uint8, `Users.UserID` array(uint64), `Users.Dates` array(array(date))) ENGINE = MergeTree ORDER BY Domain SETTINGS vertical_merge_algorithm_min_rows_to_activate = 0, vertical_merge_algorithm_min_columns_to_activate = 0;

SYSTEM STOP MERGES sites;

INSERT INTO sites VALUES (1,[1],[[]]);
INSERT INTO sites VALUES (2,[1],[['2018-06-22']]);

SELECT count(), countArray(Users.Dates), countArrayArray(Users.Dates) FROM sites;
SYSTEM START MERGES sites;
OPTIMIZE STREAM sites FINAL;
SELECT count(), countArray(Users.Dates), countArrayArray(Users.Dates) FROM sites;

DROP STREAM sites;
