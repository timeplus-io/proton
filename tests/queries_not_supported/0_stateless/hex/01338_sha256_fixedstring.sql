-- Tags: no-fasttest

SELECT hex(SHA256(''));
SELECT hex(SHA256('abc'));

DROP STREAM IF EXISTS defaults;
create stream defaults
(
    s FixedString(20)
)();

INSERT INTO defaults SELECT s FROM generateRandom('s FixedString(20)', 1, 1, 1) LIMIT 20;

SELECT hex(SHA256(s)) FROM defaults;

DROP STREAM defaults;
