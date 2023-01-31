SELECT repeat('abc', 10);
DROP STREAM IF EXISTS defaults;
CREATE STREAM defaults
(
    strings string,
    u8 uint8,
    u16 uint16,
    u32 uint32,
    u64 uint64
)ENGINE = Memory();

INSERT INTO defaults(strings,u8,u16,u32,u64) values ('abc', 3, 12, 4, 56) ('sdfgg', 2, 10, 21, 200) ('xywq', 1, 4, 9, 5) ('plkf', 0, 5, 7,77);

SELECT sleep(3);

SELECT repeat(strings, u8) FROM defaults;
SELECT repeat(strings, u16) FROM defaults;
SELECT repeat(strings, u32) from defaults;
SELECT repeat(strings, u64) FROM defaults;
SELECT repeat(strings, 10) FROM defaults;
SELECT repeat('abc', u8) FROM defaults;
SELECT repeat('abc', u16) FROM defaults;
SELECT repeat('abc', u32) FROM defaults;
SELECT repeat('abc', u64) FROM defaults;

SELECT repeat('Hello, world! ', 3);

DROP STREAM defaults;
