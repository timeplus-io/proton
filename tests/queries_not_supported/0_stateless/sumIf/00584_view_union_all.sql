SET query_mode = 'table';
drop stream IF EXISTS Test_00584;

create stream Test_00584 (
    createdDate date,
    str string,
    key enum8('A' = 0, 'B' = 1, 'ALL' = 2),
    a int64
)
ENGINE = MergeTree(createdDate, str, 8192);

INSERT INTO Test_00584(createdDate,str,key, a) VALUES ('2000-01-01', 'hello', 'A', 123);
SELECT sleep(3);
SET max_threads = 1;

CREATE VIEW TestView AS
    SELECT str, key, sum_if(a, 0) AS sum
    FROM Test_00584
    GROUP BY str, key

    UNION ALL

    SELECT str AS str, CAST('ALL' AS enum8('A' = 0, 'B' = 1, 'ALL' = 2)) AS key, sum_if(a, 0) AS sum
    FROM Test_00584
    GROUP BY str;

SELECT * FROM TestView ORDER BY key;

drop stream TestView;
drop stream Test_00584;
