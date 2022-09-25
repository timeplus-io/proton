DROP STREAM IF EXISTS sometable;

create stream sometable (
    date date,
    time int64,
    value uint64
) ENGINE=MergeTree()
ORDER BY time;


INSERT INTO sometable (date, time, value) VALUES ('2019-11-08', 1573185600, 100);

SELECT count() from sometable;

INSERT INTO sometable (date, time, value, time) VALUES ('2019-11-08', 1573185600, 100, 1573185600); -- {serverError 15}

DROP STREAM IF EXISTS sometable;
