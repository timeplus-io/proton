SELECT CAST(1 AS Enum8('hello' = 1, 'world' = 2));
SELECT cast(1 AS Enum8('hello' = 1, 'world' = 2));

SELECT CAST(1, 'Enum8(\'hello\' = 1, \'world\' = 2)');
SELECT cast(1, 'Enum8(\'hello\' = 1, \'world\' = 2)');

SELECT CAST(1 AS Enum8(
    'hello' = 1,
    'world' = 2));

SELECT cast(1 AS Enum8(
    'hello' = 1,
    'world' = 2));

SELECT CAST(1, 'Enum8(\'hello\' = 1,\n\t\'world\' = 2)');
SELECT cast(1, 'Enum8(\'hello\' = 1,\n\t\'world\' = 2)');

SELECT toTimeZone(CAST(1 AS TIMESTAMP), 'UTC');

DROP STREAM IF EXISTS cast;
create stream cast
(
    x uint8,
    e Enum8
    (
        'hello' = 1,
        'world' = 2
    )
    DEFAULT
    CAST
    (
        x
        AS
        Enum8
        (
            'hello' = 1,
            'world' = 2
        )
    )
) ENGINE = MergeTree ORDER BY e;

SHOW create stream cast FORMAT TSVRaw;
DESC STREAM cast;

INSERT INTO cast (x) VALUES (1);
SELECT * FROM cast;

DROP STREAM cast;
