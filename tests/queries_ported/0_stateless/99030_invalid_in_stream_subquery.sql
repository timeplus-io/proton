CREATE STREAM IF NOT EXISTS 99030_order
(
  `k` string,
  `k1` string,
  `k2` string,
  `v` string
)
PRIMARY KEY (k, k1, k2)
ORDER BY (k, k1, k2)
SETTINGS mode = 'versioned_kv';

CREATE STREAM IF NOT EXISTS 99030_cancel
(
  `k` string,
  `k1` string,
  `k2` string,
  `origk` string,
  `origv` string
)
PRIMARY KEY (k, k1, k2)
ORDER BY (k, k1, k2)
SETTINGS mode = 'versioned_kv';

SELECT sleep(1);
SELECT sleep(1);
SELECT sleep(1);

insert into 99030_order(k, k1, k2, v) values ('1', '2', '3', '4');
insert into 99030_cancel(k, k1, k2, origk, origv) values ('1', '1', '1', '1', '1');

SELECT sleep(1);

explain pipeline
SELECT
  k, v
FROM
  99030_order
WHERE
  k NOT IN (
    SELECT
      origk
    FROM
      99030_cancel
  ); -- { serverError NOT_IMPLEMENTED }

DROP STREAM 99030_order;
DROP STREAM 99030_cancel;
