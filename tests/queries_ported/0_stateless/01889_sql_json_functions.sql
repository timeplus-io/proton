-- Tags: no-fasttest

-- { echo }
SELECT '--json_value--';
SELECT json_value('{"hello":1}', '$'); -- root is a complex object => default value (empty string)
SELECT json_value('{"hello":1}', '$.hello');
SELECT json_value('{"hello":1.2}', '$.hello');
SELECT json_value('{"hello":true}', '$.hello');
SELECT json_value('{"hello":"world"}', '$.hello');
SELECT json_value('{"hello":null}', '$.hello');
SELECT json_value('{"hello":["world","world2"]}', '$.hello');
SELECT json_value('{"hello":{"world":"!"}}', '$.hello');
SELECT json_value('{hello:world}', '$.hello'); -- invalid json => default value (empty string)
SELECT json_value('', '$.hello');
SELECT json_value('{"foo foo":"bar"}', '$."foo foo"');
SELECT json_value('{"hello":"\\uD83C\\uDF3A \\uD83C\\uDF38 \\uD83C\\uDF37 Hello, World \\uD83C\\uDF37 \\uD83C\\uDF38 \\uD83C\\uDF3A"}', '$.hello');
SELECT json_value('{"a":"Hello \\"World\\" \\\\"}', '$.a');
select json_value('{"a":"\\n\\u0000"}', '$.a');
select json_value('{"a":"\\u263a"}', '$.a');

SELECT '--json_query--';
SELECT json_query('{"hello":1}', '$');
SELECT json_query('{"hello":1}', '$.hello');
SELECT json_query('{"hello":1.2}', '$.hello');
SELECT json_query('{"hello":true}', '$.hello');
SELECT json_query('{"hello":"world"}', '$.hello');
SELECT json_query('{"hello":null}', '$.hello');
SELECT json_query('{"hello":["world","world2"]}', '$.hello');
SELECT json_query('{"hello":{"world":"!"}}', '$.hello');
SELECT json_query( '{hello:{"world":"!"}}}', '$.hello'); -- invalid json => default value (empty string)
SELECT json_query('', '$.hello');
SELECT json_query('{"array":[[0, 1, 2, 3, 4, 5], [0, -1, -2, -3, -4, -5]]}', '$.array[*][0 to 2, 4]');

SELECT '--json_exists--';
SELECT json_exists('{"hello":1}', '$');
SELECT json_exists('', '$');
SELECT json_exists('{}', '$');
SELECT json_exists('{"hello":1}', '$.hello');
SELECT json_exists('{"hello":1,"world":2}', '$.world');
SELECT json_exists('{"hello":{"world":1}}', '$.world');
SELECT json_exists('{"hello":{"world":1}}', '$.hello.world');
SELECT json_exists('{hello:world}', '$.hello'); -- invalid json => default value (zero integer)
SELECT json_exists('', '$.hello');
SELECT json_exists('{"hello":["world"]}', '$.hello[*]');
SELECT json_exists('{"hello":["world"]}', '$.hello[0]');
SELECT json_exists('{"hello":["world"]}', '$.hello[1]');
SELECT json_exists('{"a":[{"b":1},{"c":2}]}', '$.a[*].b');
SELECT json_exists('{"a":[{"b":1},{"c":2}]}', '$.a[*].f');
SELECT json_exists('{"a":[[{"b":1}, {"g":1}],[{"h":1},{"y":1}]]}', '$.a[*][0].h');

SELECT '--MANY ROWS--';
DROP STREAM IF EXISTS 01889_sql_json;
CREATE STREAM 01889_sql_json (id uint8, json string) ENGINE = MergeTree ORDER BY id;
INSERT INTO 01889_sql_json(id, json) VALUES(0, '{"name":"Ivan","surname":"Ivanov","friends":["Vasily","Kostya","Artyom"]}');
INSERT INTO 01889_sql_json(id, json) VALUES(1, '{"name":"Katya","surname":"Baltica","friends":["Tihon","Ernest","Innokentiy"]}');
INSERT INTO 01889_sql_json(id, json) VALUES(2, '{"name":"Vitali","surname":"Brown","friends":["Katya","Anatoliy","Ivan","Oleg"]}');
SELECT id, json_query(json, '$.friends[0 to 2]') FROM 01889_sql_json ORDER BY id;
SELECT id, json_value(json, '$.friends[0]') FROM 01889_sql_json ORDER BY id;
DROP STREAM 01889_sql_json;
