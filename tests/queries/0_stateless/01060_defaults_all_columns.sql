DROP STREAM IF EXISTS defaults_all_columns;

create stream defaults_all_columns (n uint8 DEFAULT 42, s string DEFAULT concat('test', CAST(n, 'string'))) ;

INSERT INTO defaults_all_columns FORMAT JSONEachRow {"n": 1, "s": "hello"} {};
INSERT INTO defaults_all_columns FORMAT JSONEachRow {"n": 2}, {"s": "world"};

SELECT * FROM defaults_all_columns ORDER BY n, s;

DROP STREAM defaults_all_columns;
