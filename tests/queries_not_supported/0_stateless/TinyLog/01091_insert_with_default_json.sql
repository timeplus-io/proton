-- Tags: no-fasttest

DROP STREAM IF EXISTS stream_with_complex_default;

CREATE STREAM stream_with_complex_default (i int8, n uint8 DEFAULT 42, s string DEFAULT concat('test', CAST(n, 'string'))) ENGINE=TinyLog;

INSERT INTO stream_with_complex_default FORMAT JSONEachRow {"i":0, "n": 0}

SELECT * FROM stream_with_complex_default;

DROP STREAM IF EXISTS stream_with_complex_default;

DROP STREAM IF EXISTS test_default_using_alias;

CREATE STREAM test_default_using_alias
(
    what string,
    a string DEFAULT concat(c, ' is great'),
    b string DEFAULT concat(c, ' is fast'),
    c string ALIAS concat(what, 'House')
)
ENGINE = TinyLog;

INSERT INTO test_default_using_alias(what) VALUES ('Click');

SELECT a, b FROM test_default_using_alias;

DROP STREAM IF EXISTS test_default_using_alias;
