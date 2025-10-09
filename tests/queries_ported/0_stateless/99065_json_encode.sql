-- prepare data
DROP STREAM IF EXISTS stateless_99065_json;

CREATE STREAM stateless_99065_json
(
  `i` int,
  `i64` int64,
  `f` float32,
  `f64` float64,
  `s` string DEFAULT hex(random_string(20)),
  `u` uuid,
  `t` tuple(ti int, ts string),
  `ms` map(string, string),
  `ai` array(int),
  `t2` tuple(ts string, tt tuple(ti int, tb bool)),
  `ts` datetime64(3, 'UTC'),
  `ns` nullable(string)
)
ENGINE = Memory;

insert into stateless_99065_json (i, i64, f, f64, s, u, t, ms, ai, t2, ts, ns) values (10, 100, 10.01, 100.001, 'row one', 'e1440cc0-ffef-4446-9f46-5cca968235e9', (20, 'inside tuple'), {'key': 'value', 'key2': 'value2'}, [30, 31, 32], ('another tuple', (40, true)), '2025-05-07T06:29:43.678Z', 'a'), (20, 200, 20.02, 200.002, 'row two', 'c9f0af8e-da4e-4288-8906-83f42223c32e', (220, 'inside tuple 2'), {'key_2': 'value_2', 'key2_2': 'value2_2'}, [230, 231, 232], ('another tuple 2', (240, false)), '2025-05-07T07:29:43.456Z', null);

SELECT '## json_encode ##';
SELECT json_encode(i, f, s) FROM stateless_99065_json;
SELECT json_encode(*) FROM stateless_99065_json;
SELECT json_encode(1, true, 'one');
SELECT json_encode(i, 1, f, true, s, 'one') FROM stateless_99065_json;
SELECT to_type_name(json_encode());
SELECT concat('[', json_encode(), ']');

SELECT '## json_cast ##';
SELECT json_cast(i, f, s) FROM stateless_99065_json;
SELECT json_cast(*) FROM stateless_99065_json;
SELECT json_cast(1, true, 'one');
SELECT json_cast(i, 1, f, true, s, 'one') FROM stateless_99065_json;
SELECT to_type_name(json_cast(*)) FROM stateless_99065_json limit 1;
SELECT json_cast();
