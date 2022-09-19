DROP STREAM IF EXISTS visits1;
create stream visits1
(
    Sign int8,
    Arr array(int8),
    `ParsedParams.Key1` array(string),
    `ParsedParams.Key2` array(string),
    CounterID uint32
) ;

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
   select u, min(d) mn, max(d) mx, group_array(d) dg, group_array(v) vg,
       array_map(x -> x + mn, range(to_uint32(mx - mn + 1))) days,
       to_string(arrayCumSum(array_map( x -> vg[indexOf(dg, x)] , days))) cumSum
   from (select 1 u, today()-1 d, 1 v)
   group by u
);
