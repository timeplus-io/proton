WITH map(1, 'Test') AS value, 'array(tuple(uint64, string))' AS type
SELECT value, cast(value, type), cast(materialize(value), type);

WITH map(1, 'Test') AS value, 'array(tuple(uint64, uint64))' AS type
SELECT value, cast(value, type), cast(materialize(value), type); --{serverError 6}

WITH map(1, '1234') AS value, 'array(tuple(uint64, uint64))' AS type
SELECT value, cast(value, type), cast(materialize(value), type);

WITH map(1, [1, 2, 3]) AS value, 'array(tuple(uint64, array(string)))' AS type
SELECT value, cast(value, type), cast(materialize(value), type);

WITH map(1, ['1', '2', '3']) AS value, 'array(tuple(uint64, array(uint64)))' AS type
SELECT value, cast(value, type), cast(materialize(value), type);

WITH map(1, map(1, '1234')) AS value, 'array(tuple(uint64, Map(uint64, string)))' AS type
SELECT value, cast(value, type), cast(materialize(value), type);

WITH map(1, map(1, '1234')) AS value, 'array(tuple(uint64, Map(uint64, uint64)))' AS type
SELECT value, cast(value, type), cast(materialize(value), type);

WITH map(1, map(1, '1234')) AS value, 'array(tuple(uint64, array(tuple(uint64, string))))' AS type
SELECT value, cast(value, type), cast(materialize(value), type);

WITH map(1, map(1, '1234')) as value, 'array(tuple(uint64, array(tuple(uint64, uint64))))' AS type
SELECT value, cast(value, type), cast(materialize(value), type);
