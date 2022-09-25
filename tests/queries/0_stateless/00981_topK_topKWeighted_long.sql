-- Tags: long

DROP STREAM IF EXISTS topk;

create stream topk (val1 string, val2 uint32) ENGINE = MergeTree ORDER BY val1;

INSERT INTO topk WITH number % 7 = 0 AS frequent SELECT to_string(frequent ? number % 10 : number), frequent ? 999999999 : number FROM numbers(4000000);

SELECT arraySort(top_k(10)(val1)) FROM topk;
SELECT arraySort(topKWeighted(10)(val1, val2)) FROM topk;
SELECT topKWeighted(10)(to_string(number), number) from numbers(3000000);

DROP STREAM topk;
