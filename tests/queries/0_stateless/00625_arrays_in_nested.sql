-- Tags: no-parallel

DROP STREAM IF EXISTS nested;
create stream nested
(
    column nested
    (
        name string,
        names array(string),
        types array(Enum8('PU' = 1, 'US' = 2, 'OTHER' = 3))
    )
) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO nested VALUES (['Hello', 'World'], [['a'], ['b', 'c']], [['PU', 'US'], ['OTHER']]);

SELECT * FROM nested;

DETACH TABLE nested;
ATTACH TABLE nested;

SELECT * FROM nested;

INSERT INTO nested VALUES (['GoodBye'], [['1', '2']], [['PU', 'US', 'OTHER']]);

SELECT * FROM nested ORDER BY column.name;
OPTIMIZE STREAM nested PARTITION tuple() FINAL;
SELECT * FROM nested ORDER BY column.name;

DETACH TABLE nested;
ATTACH TABLE nested;

SELECT * FROM nested ORDER BY column.name;


DROP STREAM IF EXISTS nested;
create stream nested
(
    column nested
    (
        name string,
        names array(string),
        types array(Enum8('PU' = 1, 'US' = 2, 'OTHER' = 3))
    )
)  ;

INSERT INTO nested VALUES (['Hello', 'World'], [['a'], ['b', 'c']], [['PU', 'US'], ['OTHER']]);

SELECT * FROM nested;


DROP STREAM IF EXISTS nested;
create stream nested
(
    column nested
    (
        name string,
        names array(string),
        types array(Enum8('PU' = 1, 'US' = 2, 'OTHER' = 3))
    )
) ;

INSERT INTO nested VALUES (['Hello', 'World'], [['a'], ['b', 'c']], [['PU', 'US'], ['OTHER']]);

SELECT * FROM nested;


DROP STREAM IF EXISTS nested;
create stream nested
(
    column nested
    (
        name string,
        names array(string),
        types array(Enum8('PU' = 1, 'US' = 2, 'OTHER' = 3))
    )
) ENGINE = StripeLog;

INSERT INTO nested VALUES (['Hello', 'World'], [['a'], ['b', 'c']], [['PU', 'US'], ['OTHER']]);

SELECT * FROM nested;


DROP STREAM IF EXISTS nested;
create stream nested
(
    column nested
    (
        name string,
        names array(string),
        types array(Enum8('PU' = 1, 'US' = 2, 'OTHER' = 3))
    )
) ;

INSERT INTO nested VALUES (['Hello', 'World'], [['a'], ['b', 'c']], [['PU', 'US'], ['OTHER']]);

SELECT * FROM nested;


DROP STREAM nested;
