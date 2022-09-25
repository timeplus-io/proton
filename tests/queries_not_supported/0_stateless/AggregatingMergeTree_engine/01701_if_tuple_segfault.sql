DROP STREAM IF EXISTS agg_table;

create stream IF NOT EXISTS agg_table
(
    time DateTime CODEC(DoubleDelta, LZ4),
    xxx string,
    two_values tuple(array(uint16), uint32),
    agg_simple SimpleAggregateFunction(sum, uint64),
    agg SimpleAggregateFunction(sumMap, tuple(array(Int16), array(uint64)))
)
ENGINE = AggregatingMergeTree()
ORDER BY (xxx, time);

INSERT INTO agg_table SELECT to_datetime('2020-10-01 19:20:30'), 'hello', ([any(number)], sum(number)), sum(number),
    sumMap((array_map(i -> to_string(i), range(13)), array_map(i -> (number + i), range(13)))) FROM numbers(10);

SELECT * FROM agg_table;

SELECT if(xxx = 'x', ([2], 3), ([3], 4)) FROM agg_table;

SELECT if(xxx = 'x', ([2], 3), ([3], 4, 'q', 'w', 7)) FROM agg_table; --{ serverError 386 }

ALTER STREAM agg_table UPDATE two_values = (two_values.1, two_values.2) WHERE time BETWEEN to_datetime('2020-08-01 00:00:00') AND to_datetime('2020-12-01 00:00:00') SETTINGS mutations_sync = 2;

ALTER STREAM agg_table UPDATE agg_simple = 5 WHERE time BETWEEN to_datetime('2020-08-01 00:00:00') AND to_datetime('2020-12-01 00:00:00') SETTINGS mutations_sync = 2;

ALTER STREAM agg_table UPDATE agg = (agg.1, agg.2) WHERE time BETWEEN to_datetime('2020-08-01 00:00:00') AND to_datetime('2020-12-01 00:00:00') SETTINGS mutations_sync = 2;

ALTER STREAM agg_table UPDATE agg = (agg.1, array_map(x -> to_uint64(x / 2), agg.2)) WHERE time BETWEEN to_datetime('2020-08-01 00:00:00') AND to_datetime('2020-12-01 00:00:00') SETTINGS mutations_sync = 2;

SELECT * FROM agg_table;

DROP STREAM IF EXISTS agg_table;
