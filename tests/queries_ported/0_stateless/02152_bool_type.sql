SELECT CAST('True', 'bool');
SELECT CAST('TrUe', 'bool');
SELECT CAST('true', 'bool');
SELECT CAST('On', 'bool');
SELECT CAST('on', 'bool');
SELECT CAST('Yes', 'bool');
SELECT CAST('yes', 'bool');
SELECT CAST('T', 'bool');
SELECT CAST('t', 'bool');
SELECT CAST('Y', 'bool');
SELECT CAST('y', 'bool');
SELECT CAST('1', 'bool');
SELECT CAST('enabled', 'bool');
SELECT CAST('enable', 'bool');

SELECT CAST('False', 'bool');
SELECT CAST('FaLse', 'bool');
SELECT CAST('false', 'bool');
SELECT CAST('Off', 'bool');
SELECT CAST('off', 'bool');
SELECT CAST('No', 'bool');
SELECT CAST('no', 'bool');
SELECT CAST('N', 'bool');
SELECT CAST('n', 'bool');
SELECT CAST('F', 'bool');
SELECT CAST('f', 'bool');
SELECT CAST('0', 'bool');
SELECT CAST('disabled', 'bool');
SELECT CAST('disable', 'bool');

SET bool_true_representation = 'Custom true';
SET bool_false_representation = 'Custom false';

SELECT CAST('true', 'bool') format CSV;
SELECT CAST('true', 'bool') format TSV;
SELECT CAST('true', 'bool') format Values;
SELECT '';
SELECT CAST('true', 'bool') format Vertical;
SELECT CAST('true', 'bool') format Pretty;
SELECT CAST('true', 'bool') format JSONEachRow;

SELECT CAST(CAST(2, 'bool'), 'uint8');
SELECT CAST(CAST(to_uint32(2), 'bool'), 'uint8');
SELECT CAST(CAST(to_int8(2), 'bool'), 'uint8');
SELECT CAST(CAST(to_float32(2), 'bool'), 'uint8');
SELECT CAST(CAST(to_decimal32(2, 2), 'bool'), 'uint8');
SELECT CAST(CAST(materialize(2), 'bool'), 'uint8');

