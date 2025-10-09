drop view if exists 99016_mv;
drop stream if exists 99016_ipfix;
drop stream if exists 99016_victims;

CREATE STREAM 99016_ipfix
(
 `ipv4_src_ip` int32,
 `ipv4_dst_ip` int32,
 `start_time` int32,
 `end_time` int32,
 `pkt_count` int32,
 `byte_count` int32,
 `l4_src_port` int16,
 `l4_dst_port` int16,
 `protocol` int8,
 `tcp_flags` int8,
 `tags` string,
 `input_if_id` int16,
 `output_if_id` int16,
 `_tp_time` datetime64(3, 'UTC') DEFAULT now64(3, 'UTC') CODEC(DoubleDelta, LZ4),
 INDEX _tp_time_index _tp_time TYPE minmax GRANULARITY 256
)
ENGINE = Stream(4, weak_hash32(ipv4_dst_ip))
PARTITION BY to_YYYYMMDD(_tp_time)
ORDER BY to_start_of_hour(_tp_time)
SETTINGS shards = 4, sharding_expr = 'weak_hash32(ipv4_dst_ip)', flush_threshold_count = 10000000, merge_with_ttl_timeout = 120, logstore_retention_bytes = 8589934592, logstore_retention_ms = 120000, index_granularity = 8192;

CREATE STREAM 99016_victims
(
 `victim_ip` uint32,
 `sum_bytes` uint64,
 `detect_start` datetime64(3),
 `_tp_time` datetime64(3, 'UTC') DEFAULT now64(3, 'UTC') CODEC(DoubleDelta, LZ4),
 INDEX _tp_time_index _tp_time TYPE minmax GRANULARITY 256
)
ENGINE = Stream(1, rand())
PARTITION BY to_YYYYMMDD(_tp_time)
ORDER BY to_start_of_hour(_tp_time)
SETTINGS shards = 1, logstore_retention_bytes = 8589934592, logstore_retention_ms = 120000, index_granularity = 8192;

CREATE MATERIALIZED VIEW 99016_mv1 AS SELECT * from table(99016_ipfix); -- { serverError INCORRECT_QUERY }
CREATE MATERIALIZED VIEW 99016_mv2 INTO 99016_victims AS SELECT ipv4_dst_ip AS victim_ip, sum(byte_count) AS sum_bytes, window_start AS detect_start FROM hop(99016_ipfix, 1s, 30s) GROUP BY window_start, ipv4_dst_ip HAVING sum_bytes SETTINGS allow_independent_shard_processing = true; -- { serverError ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER }
CREATE MATERIALIZED VIEW 99016_mv3 AS SELECT * FROM 99016_ipfix STORAGE_SETTINGS logstore_retention_byte=1073741824, logstore_retention_ms=216000000 TTL to_datetime(_tp_time) + INTERVAL 36 DAY; -- { serverError UNKNOWN_SETTING }

--- https://github.com/timeplus-io/proton-enterprise/issues/9818
CREATE MATERIALIZED VIEW 99016_mv4 AS SELECT ipv4_dst_ip AS victim_ip, sum(byte_count) AS sum_bytes, window_start AS detect_start FROM tumble(99016_ipfix, 30s) group by window_start, ipv4_dst_ip EMIT TIMEOUT 30s;
CREATE MATERIALIZED VIEW 99016_mv5 AS SELECT ipv4_dst_ip AS victim_ip, sum(byte_count) AS sum_bytes FROM 99016_ipfix GROUP BY ipv4_dst_ip EMIT TIMEOUT 30s;

drop view if exists 99016_mv1;
drop view if exists 99016_mv2;
drop view if exists 99016_mv3;
drop view if exists 99016_mv4;
drop view if exists 99016_mv5;
drop stream if exists 99016_ipfix;
drop stream if exists 99016_victims;
