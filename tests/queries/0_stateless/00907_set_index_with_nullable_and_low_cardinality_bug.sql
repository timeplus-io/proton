SET query_mode = 'table';
drop stream if exists null_lc_set_index;

create stream null_lc_set_index (
  timestamp         DateTime,
  action            LowCardinality(Nullable(string)),
  user              LowCardinality(Nullable(string)),
  INDEX test_user_idx (user) TYPE set(0) GRANULARITY 8192
) ENGINE=MergeTree
  PARTITION BY toYYYYMMDD(timestamp)
  ORDER BY (timestamp, action, cityHash64(user)) SETTINGS allow_nullable_key = 1;
INSERT INTO null_lc_set_index VALUES (1550883010, 'subscribe', 'alice');
INSERT INTO null_lc_set_index VALUES (1550883020, 'follow', 'bob');

SELECT action, user FROM null_lc_set_index WHERE user = 'alice';

drop stream if exists null_lc_set_index;

