DROP STREAM IF EXISTS enum_pk;
create stream enum_pk (date date DEFAULT '0000-00-00', x Enum8('0' = 0, '1' = 1, '2' = 2), d Enum8('0' = 0, '1' = 1, '2' = 2)) ENGINE = MergeTree(date, x, 1);
INSERT INTO enum_pk (x, d) VALUES ('0', '0')('1', '1')('0', '0')('1', '1')('1', '1')('0', '0')('0', '0')('2', '2')('0', '0')('1', '1')('1', '1')('1', '1')('1', '1')('0', '0');

SELECT cityHash64(group_array(x)) FROM enum_pk WHERE x = '0';
SELECT cityHash64(group_array(d)) FROM enum_pk WHERE d = '0';

SELECT cityHash64(group_array(x)) FROM enum_pk WHERE x != '0';
SELECT cityHash64(group_array(d)) FROM enum_pk WHERE d != '0';

SELECT cityHash64(group_array(x)) FROM enum_pk WHERE x = '1';
SELECT cityHash64(group_array(d)) FROM enum_pk WHERE d = '1';

SELECT cityHash64(group_array(x)) FROM enum_pk WHERE exp2(to_int64(x != '1')) > 1;
SELECT cityHash64(group_array(d)) FROM enum_pk WHERE exp2(to_int64(d != '1')) > 1;

SELECT cityHash64(group_array(x)) FROM enum_pk WHERE x = to_string(0);
SELECT cityHash64(group_array(d)) FROM enum_pk WHERE d = to_string(0);

SELECT cityHash64(group_array(x)) FROM enum_pk WHERE (x = to_string(0)) > 0;
SELECT cityHash64(group_array(d)) FROM enum_pk WHERE (d = to_string(0)) > 0;

SELECT cityHash64(group_array(x)) FROM enum_pk WHERE ((x != to_string(1)) > 0) > 0;
SELECT cityHash64(group_array(d)) FROM enum_pk WHERE ((d != to_string(1)) > 0) > 0;

SELECT cityHash64(group_array(x)) FROM enum_pk WHERE exp2((x != to_string(0)) != 0) > 1;
SELECT cityHash64(group_array(d)) FROM enum_pk WHERE exp2((d != to_string(0)) != 0) > 1;

SELECT cityHash64(group_array(x)) FROM enum_pk WHERE (-(x != to_string(0)) = -1) > 0;
SELECT cityHash64(group_array(d)) FROM enum_pk WHERE (-(d != to_string(0)) = -1) > 0;

SELECT cityHash64(group_array(x)) FROM enum_pk WHERE 1 = 1;
SELECT cityHash64(group_array(d)) FROM enum_pk WHERE 1 = 1;

SELECT cityHash64(group_array(x)) FROM enum_pk WHERE (x = '0' OR x = '1');
SELECT cityHash64(group_array(d)) FROM enum_pk WHERE (d = '0' OR d = '1');

SELECT cityHash64(group_array(x)) FROM enum_pk WHERE x IN ('0', '1');
SELECT cityHash64(group_array(d)) FROM enum_pk WHERE d IN ('0', '1');

SELECT cityHash64(group_array(x)) FROM enum_pk WHERE (x != '0' AND x != '1');
SELECT cityHash64(group_array(d)) FROM enum_pk WHERE (d != '0' AND d != '1');

DROP STREAM enum_pk;
