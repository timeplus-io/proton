DROP STREAM IF EXISTS mutations_and_escaping_1648;

create stream mutations_and_escaping_1648 (d date, e Enum8('foo'=1, 'bar'=2)) Engine = MergeTree(d, (d), 8192);
INSERT INTO mutations_and_escaping_1648 (d, e) VALUES ('2018-01-01', 'foo');
INSERT INTO mutations_and_escaping_1648 (d, e) VALUES ('2018-01-02', 'bar');

-- slow mutation
ALTER STREAM mutations_and_escaping_1648 UPDATE e = CAST('foo', 'Enum8(\'foo\' = 1, \'bar\' = 2)') WHERE d='2018-01-02' and sleepEachRow(1) = 0;

-- check that we able to read mutation text after serialization
DETACH STREAM mutations_and_escaping_1648;
ATTACH STREAM mutations_and_escaping_1648;

ALTER STREAM mutations_and_escaping_1648 UPDATE e = CAST('foo', 'Enum8(\'foo\' = 1, \'bar\' = 2)') WHERE d='2018-01-02' SETTINGS mutations_sync = 1;

SELECT e FROM mutations_and_escaping_1648 ORDER BY d;

DROP STREAM mutations_and_escaping_1648;
