DROP STREAM IF EXISTS visits1;
CREATE STREAM visits1
(
    Sign int8,
    Arr array(int8),
    `ParsedParams.Key1` array(string),
    `ParsedParams.Key2` array(string),
    CounterID uint32
) ENGINE = Memory;

SELECT array_map(x -> x * Sign, Arr) FROM visits1;

SELECT PP.Key2 AS `ym:s:pl2`
FROM visits1
ARRAY JOIN
    `ParsedParams.Key2` AS `PP.Key2`,
    `ParsedParams.Key1` AS `PP.Key1`,
    array_enumerate_uniq(`ParsedParams.Key2`, array_map(x_0 -> 1, `ParsedParams.Key1`)) AS `upp_==_yes_`,
    array_enumerate_uniq(`ParsedParams.Key2`) AS _uniq_ParsedParams
WHERE CounterID = 100500;

DROP STREAM visits1;

select u, cumSum from (
   select u, min(d) as mn, max(d) as mx, group_array(d) as dg, group_array(v) as vg,
       array_map(x -> x + mn, range(to_uint32(mx - mn + 1))) as days,
       to_string(array_cum_sum(array_map( x -> vg[index_of(dg, x)] , days))) as cumSum
   from (select 1 as u, today()-1 as d, 1 as v)
   group by u
);
