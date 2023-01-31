DROP TABLE IF EXISTS visits1;
CREATE TABLE visits1
(
    Sign Int8,
    Arr Array(Int8),
    `ParsedParams.Key1` Array(String),
    `ParsedParams.Key2` Array(String),
    CounterID UInt32
) ENGINE = Memory;

SELECT arrayMap(x -> x * Sign, Arr) FROM visits1;

SELECT PP.Key2 AS `ym:s:pl2`
FROM visits1
ARRAY JOIN
    `ParsedParams.Key2` AS `PP.Key2`,
    `ParsedParams.Key1` AS `PP.Key1`,
    arrayEnumerateUniq(`ParsedParams.Key2`, arrayMap(x_0 -> 1, `ParsedParams.Key1`)) AS `upp_==_yes_`,
    arrayEnumerateUniq(`ParsedParams.Key2`) AS _uniq_ParsedParams
WHERE CounterID = 100500;

DROP TABLE visits1;

select u, cumSum from (
   select u, min(d) as mn, max(d) as mx, group_array(d) as dg, group_array(v) as vg,
       array_map(x -> x + mn, range(to_uint32(mx - mn + 1))) as days,
       to_string(array_cum_sum(array_map( x -> vg[index_of(dg, x)] , days))) as cumSum
   from (select 1 as u, today()-1 as d, 1 as v)
   group by u
);
