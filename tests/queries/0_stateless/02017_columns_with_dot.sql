DROP STREAM IF EXISTS t_with_dots;
create stream t_with_dots (id uint32, arr array(uint32), `b.id` uint32, `b.arr` array(uint32))  ;

INSERT INTO t_with_dots VALUES (1, [0, 0], 2, [1, 1, 3]);
SELECT * FROM t_with_dots;

DROP STREAM t_with_dots;

create stream t_with_dots (id uint32, arr array(uint32), `b.id` uint32, `b.arr` array(uint32))
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_with_dots VALUES (1, [0, 0], 2, [1, 1, 3]);
SELECT * FROM t_with_dots;

DROP STREAM t_with_dots;

create stream t_with_dots (id uint32, arr array(uint32), `b.id` uint32, `b.arr` array(uint32))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_with_dots VALUES (1, [0, 0], 2, [1, 1, 3]);
SELECT * FROM t_with_dots;

DROP STREAM t_with_dots;
