DROP STREAM IF EXISTS cdp_orders;

create stream cdp_orders
(
    `order_id` string,
    `order_status` string,
    `order_time` datetime
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMMDD(order_time)
ORDER BY (order_time, order_id)
SETTINGS index_granularity = 8192;

INSERT INTO cdp_orders VALUES ('hello', 'world', '2020-01-02 03:04:05');

SELECT * FROM cdp_orders;
SET mutations_sync = 1;
ALTER STREAM cdp_orders DELETE WHERE order_time >= '2019-12-03 00:00:00';
SELECT * FROM cdp_orders;

DROP STREAM cdp_orders;
