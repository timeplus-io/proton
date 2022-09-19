-- Tags: no-fasttest

DROP STREAM IF EXISTS table_with_complex_default;

create stream table_with_complex_default (i int8, n uint8 DEFAULT 42, s string DEFAULT concat('test', CAST(n, 'string'))) ;

INSERT INTO table_with_complex_default FORMAT JSONEachRow {"i":0, "n": 0}

SELECT * FROM table_with_complex_default;

DROP STREAM IF EXISTS table_with_complex_default;

DROP STREAM IF EXISTS test_default_using_alias;

create stream test_default_using_alias
(
    what string,
    a string DEFAULT concat(c, ' is great'),
    b string DEFAULT concat(c, ' is fast'),
    c string ALIAS concat(what, 'House')
)
;

INSERT INTO test_default_using_alias(what) VALUES ('Click');

SELECT a, b FROM test_default_using_alias;

DROP STREAM IF EXISTS test_default_using_alias;
