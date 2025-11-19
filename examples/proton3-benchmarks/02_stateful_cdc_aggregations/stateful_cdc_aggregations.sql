-- Stateful CDC aggregations benchmark.

CREATE STREAM default.changelog_transfers
(
  `from_id` string,
  `to_id`   string,
  `value`   int256
)
SETTINGS mode = 'changelog';

CREATE RANDOM STREAM rand_transfers
(
  `from_id`   string DEFAULT concat('account_', to_string(rand() % 40000000)),
  `to_id`     string DEFAULT concat('account_', to_string(rand() % 40000000)),
  `value`     int256 DEFAULT to_int256(1 + (rand64() % 100000)),
  `_tp_delta` int8   DEFAULT 1
);

CREATE MATERIALIZED VIEW mv
INTO default.changelog_transfers AS
SELECT
  from_id,
  to_id,
  value,
  _tp_delta
FROM rand_transfers
SETTINGS eps = 6000000;

SELECT
  account_id,
  sum(total) AS balance
FROM
(
  SELECT
    array_join(accounts) AS account_id,
    if(account_id = from_id, -value, value) AS total,
    _tp_delta
  FROM
  (
    SELECT
      from_id,
      to_id,
      [from_id, to_id] AS accounts,
      value,
      _tp_delta
    FROM default.changelog_transfers
  )
)
GROUP BY account_id
EMIT STREAM ON UPDATE;

