DROP STREAM IF EXISTS final_test;
create stream final_test (id string, version date) ENGINE = ReplacingMergeTree(version, id, 8192);
INSERT INTO final_test (id, version) VALUES ('2018-01-01', '2018-01-01');
SELECT * FROM final_test FINAL PREWHERE id == '2018-01-02';
DROP STREAM final_test;
