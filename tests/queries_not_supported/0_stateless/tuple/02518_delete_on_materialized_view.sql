DROP STREAM IF EXISTS kek;
DROP STREAM IF EXISTS kekv;

CREATE STREAM kek (a uint32) ENGINE = MergeTree ORDER BY a;
CREATE MATERIALIZED VIEW kekv ENGINE = MergeTree ORDER BY tuple() AS SELECT * FROM kek;

INSERT INTO kek VALUES (1);
DELETE FROM kekv WHERE a = 1; -- { serverError BAD_ARGUMENTS}

SET allow_experimental_lightweight_delete=1;
DELETE FROM kekv WHERE a = 1; -- { serverError BAD_ARGUMENTS}

DROP STREAM IF EXISTS kek;
DROP STREAM IF EXISTS kekv;
