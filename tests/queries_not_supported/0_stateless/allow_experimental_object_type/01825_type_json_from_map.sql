-- Tags: no-fasttest

DROP STREAM IF EXISTS t_json;
DROP STREAM IF EXISTS t_map;

SET allow_experimental_object_type = 1;

CREATE STREAM t_json(id uint64, obj JSON) ENGINE = MergeTree ORDER BY id;
CREATE STREAM t_map(id uint64, m map(string, uint64)) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_map
SELECT
    number,
    (
        array_map(x -> 'col' || to_string(x), range(number % 10)),
        range(number % 10)
    )::map(string, uint64)
FROM numbers(1000000);

INSERT INTO t_json SELECT id, m FROM t_map;
SELECT sum(m['col1']), sum(m['col4']), sum(m['col7']), sum(m['col8'] = 0) FROM t_map;
SELECT sum(obj.col1), sum(obj.col4), sum(obj.col7), sum(obj.col8 = 0) FROM t_json;
SELECT to_type_name(obj) FROM t_json LIMIT 1;

INSERT INTO t_json
SELECT
    number,
    (
        array_map(x -> 'col' || to_string(x), range(number % 10)),
        range(number % 10)
    )::map(fixed_string(4), uint64)
FROM numbers(1000000);

SELECT sum(obj.col1), sum(obj.col4), sum(obj.col7), sum(obj.col8 = 0) FROM t_json;

INSERT INTO t_json
SELECT number, (range(number % 10), range(number % 10))::map(uint64, uint64)
FROM numbers(1000000); -- { serverError 53 }

DROP STREAM IF EXISTS t_json;
DROP STREAM IF EXISTS t_map;
