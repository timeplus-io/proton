-- Tags: zookeeper

DROP STREAM IF EXISTS t_remove_sample_by;

create stream t_remove_sample_by(id uint64) ENGINE = MergeTree ORDER BY id SAMPLE BY id;

ALTER STREAM t_remove_sample_by REMOVE SAMPLE BY;
SHOW create stream t_remove_sample_by;

ALTER STREAM t_remove_sample_by REMOVE SAMPLE BY; -- { serverError 36 }
SELECT * FROM t_remove_sample_by SAMPLE 1 / 10; -- { serverError 141 }

DROP STREAM t_remove_sample_by;

create stream t_remove_sample_by(id uint64)
ENGINE = ReplicatedMergeTree('/clickhouse/{database}/t_remove_sample_by', '1')
ORDER BY id SAMPLE BY id;

ALTER STREAM t_remove_sample_by REMOVE SAMPLE BY;
SHOW create stream t_remove_sample_by;

DROP STREAM t_remove_sample_by;

create stream t_remove_sample_by(id uint64) ;
ALTER STREAM t_remove_sample_by REMOVE SAMPLE BY; -- { serverError 36 }

DROP STREAM t_remove_sample_by;

create stream t_remove_sample_by(id string)
ENGINE = MergeTree ORDER BY id SAMPLE BY id
SETTINGS check_sample_column_is_correct = 0;

ALTER STREAM t_remove_sample_by RESET SETTING check_sample_column_is_correct;

DETACH TABLE t_remove_sample_by;
ATTACH TABLE t_remove_sample_by;

INSERT INTO t_remove_sample_by VALUES (1);
SELECT * FROM t_remove_sample_by SAMPLE 1 / 10; -- { serverError 59 }

ALTER STREAM t_remove_sample_by REMOVE SAMPLE BY;
SHOW create stream t_remove_sample_by;

DROP STREAM t_remove_sample_by;
