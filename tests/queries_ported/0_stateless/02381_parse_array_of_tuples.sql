DROP STREAM IF EXISTS t_parse_tuples;

CREATE STREAM t_parse_tuples
(
    id uint32,
    arr array(array(tuple(c1 int32, c2 uint8)))
)
ENGINE = Memory;

INSERT INTO t_parse_tuples VALUES (1, [[]]), (2, [[(500, -10)]]), (3, [[(500, '10')]]);

SELECT * FROM t_parse_tuples ORDER BY id;

DROP STREAM IF EXISTS t_parse_tuples;
