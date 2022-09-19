SET optimize_read_in_order = 1;
DROP STREAM IF EXISTS order_with_aggr;
create stream order_with_aggr(a int) ENGINE = MergeTree ORDER BY a;

INSERT INTO order_with_aggr SELECT * FROM numbers(100);
SELECT sum(a) as s FROM order_with_aggr ORDER BY s;

DROP STREAM order_with_aggr;
