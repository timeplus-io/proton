-- Tags: long

DROP STREAM IF EXISTS tvs;

create stream tvs(k uint32, t uint32, tv uint64) ;
INSERT INTO tvs(k,t,tv) SELECT k, t, t
FROM (SELECT to_uint32(number) AS k FROM numbers(1000)) keys
CROSS JOIN (SELECT to_uint32(number * 3) as t FROM numbers(10000)) tv_times;

SELECT SUM(trades.price - tvs.tv) FROM
(SELECT k, t, t as price
    FROM (SELECT to_uint32(number) AS k FROM numbers(1000)) keys
    CROSS JOIN (SELECT to_uint32(number * 10) AS t FROM numbers(3000)) trade_times) trades
ASOF LEFT JOIN tvs USING(k,t);

DROP STREAM tvs;
