DROP STREAM IF EXISTS map_lc;
SET allow_experimental_map_type = 1;
create stream map_lc
(
    `kv` Map(low_cardinality(string), low_cardinality(string))
)
;

INSERT INTO map_lc select map('a', 'b');
SELECT kv['a'] FROM map_lc;
DROP STREAM map_lc;
SELECT map(to_fixed_string('1',1),1) AS m, m[to_fixed_string('1',1)],m[to_fixed_string('1',2)];
