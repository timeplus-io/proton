SELECT CAST(1 AS enum8('hello' = 1, 'world' = 2));
SELECT cast(1 AS enum8('hello' = 1, 'world' = 2));

SELECT CAST(1, 'enum8(\'hello\' = 1, \'world\' = 2)');
SELECT cast(1, 'enum8(\'hello\' = 1, \'world\' = 2)');

SELECT CAST(1 AS enum8(
    'hello' = 1,
    'world' = 2));

SELECT cast(1 AS enum8(
    'hello' = 1,
    'world' = 2));

SELECT CAST(1, 'enum8(\'hello\' = 1,\n\t\'world\' = 2)');
SELECT cast(1, 'enum8(\'hello\' = 1,\n\t\'world\' = 2)');

SELECT to_timezone(CAST(1 AS datetime), 'UTC');

DROP STREAM IF EXISTS cast;
create stream cast
(
    x uint8,
    e enum8
    (
        'hello' = 1,
        'world' = 2
    )
    DEFAULT
    CAST
    (
        x
        AS
        enum8
        (
            'hello' = 1,
            'world' = 2
        )
    )
) ENGINE = MergeTree ORDER BY e;

SHOW create cast FORMAT TSVRaw;
DESC STREAM cast;

INSERT INTO cast (x) VALUES (1);
SELECT * FROM cast;

DROP STREAM cast;
