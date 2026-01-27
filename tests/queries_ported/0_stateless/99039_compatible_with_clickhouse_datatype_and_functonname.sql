SET is_clickhouse_compatible = 1;
 
DROP STREAM IF EXISTS test_stream_1 ;
DROP STREAM IF EXISTS user_profiles ;
CREATE STREAM IF NOT EXISTS test_stream_1 
(
    id UInt32,
    name String,
    created_at DateTime DEFAULT '2025-03-10 14:43:21',
    is_active Boolean,
    value Float64,
    tags Array(String),
    metadata Map(String, String),
    options Enum('small' = 1, 'medium' = 2, 'large' = 3),
    addresses Array(
        Tuple(String, String)
    ),
    preferences Map(String, Array(Int32))
) ENGINE=MergeTree ORDER BY id;

CREATE STREAM IF NOT EXISTS user_profiles 
(
    id UInt32,
    profile String
) ENGINE = MergeTree()
ORDER BY id;


INSERT INTO test_stream_1  (id, name, is_active, value, tags, metadata, options, addresses, preferences) VALUES (1, 'Alice', true, 99.5, ['tag1', 'tag2'], {'key1': 'value1', 'key2': 'value2'}, 'medium', [('New York', '5th Ave'), ('Los Angeles', 'Sunset Blvd')], {'movies': [1, 2, 3], 'books': [4, 5]});
INSERT INTO test_stream_1  (id, name, is_active, value, tags, metadata, options, addresses, preferences) VALUES (2, 'Bob', false, 45.3, ['tag3'], {'key3': 'value3'}, 'small', [('Chicago', 'State St')], {'movies': [6, 7], 'books': [8, 9]});
INSERT INTO test_stream_1  (id, name, is_active, value, tags, metadata, options, addresses, preferences) VALUES (3, 'Charlie', true, 78.2, ['tag4', 'tag5', 'tag6'], {'key4': 'value4', 'key5': 'value5'}, 'large', [('Miami', 'Ocean Dr')], {'movies': [10], 'books': [11, 12]});

INSERT INTO user_profiles  (id, profile) VALUES (1, 'Admin'), (2, 'Guest'), (3, 'Member');



-- clickhouse
-- TabSeparatedWithNamesAndTypes
SELECT * FROM test_stream_1  order by id FORMAT TabSeparatedWithNamesAndTypes;
SELECT id, lower(name) FROM test_stream_1  order by id FORMAT TabSeparatedWithNamesAndTypes;
SELECT id, name, CAST(is_active AS Int32) AS is_active_int, CAST(value AS Int32) AS value_int, CAST(tags AS String) AS tags_string FROM test_stream_1 ORDER BY id;
SELECT id, name, toDate(created_at) AS created_date, toStartOfMonth(created_at) AS month_start, toDatetime('2025-03-12 14:03:00') - created_at AS time_diff FROM test_stream_1 ORDER BY id;
SELECT toDate(created_at) AS created_date, COUNT(*) AS records_count, AVG(value) AS average_value FROM test_stream_1  GROUP BY created_date ORDER BY created_date;
SELECT id, CAST(arrayMap(tag -> (tag, length(tag)), tags) AS Array(Tuple(String, Int32))) AS tag_with_lengths FROM test_stream_1  order by id;
SELECT id, CAST((dateDiff('day', toDate(created_at), toDate(toDatetime('2025-03-12 14:03:00'))), name) AS Tuple(Int32, String)) AS date_diff_and_name FROM test_stream_1 order by id;
SELECT id, CAST(concat(CAST(id AS String), '_', name) AS String) AS combined_id_name FROM test_stream_1 order by id;
SELECT id, name, CAST(arrayMap(tag -> (tag, 0), tags) AS Array(Tuple(String, Int32))) AS tags_with_index FROM test_stream_1 order by id;
SELECT id, name, value FROM test_stream_1  WHERE value > (SELECT AVG(value) FROM test_stream_1 ) order by id;
SELECT id, name, sum(arraySum(preferences['movies'])) AS total_movies FROM test_stream_1 WHERE is_active = true GROUP BY id, name order by id;

-- JOIN
SELECT ts.options, up.profile, COUNT(*) AS active_users_count, SUM(ts.value) AS total_value, arrayJoin(ts.tags) AS tag, COUNT(DISTINCT ts.id) AS users_in_tag, MAX(ts.created_at) AS last_activity FROM test_stream_1  ts JOIN user_profiles  up ON ts.id = up.id WHERE ts.is_active = 1 GROUP BY ts.options, up.profile, tag ORDER BY ts.options, tag;
SELECT ts.id, ts.name, ts.options, ts.value, CAST(ts.created_at AS Nullable(DateTime64(3))) as new_created_at, ts.is_active, up.profile FROM test_stream_1  ts JOIN user_profiles  up ON ts.id = up.id ORDER BY ts.id;

-- subquery
SELECT id, name, value, CAST(toDate(created_at) AS Date) AS created_date FROM test_stream_1  WHERE id IN (SELECT id FROM test_stream_1  WHERE options = 'large' AND value > 50) AND is_active = true ORDER BY value DESC;
SELECT id, name, value, options, toHour(created_at) AS created_hour FROM test_stream_1  WHERE id IN (SELECT id FROM test_stream_1  WHERE toStartOfDay(created_at) = toStartOfDay(toDate('2025-03-10'))) AND is_active = true ORDER BY created_hour, id;
SELECT id, name, value, toDate(created_at) AS created_date, addDays(toDate(created_at), 7) AS date_plus_seven FROM test_stream_1  WHERE id IN (SELECT id FROM test_stream_1  WHERE value > 50) AND is_active = true ORDER BY created_date;


-- timeplus
SELECT id, lower(name) FROM test_stream_1  order by id FORMAT TabSeparatedWithNamesAndTypes;
SELECT id, name, CAST(is_active AS int32) AS is_active_int, CAST(value AS int32) AS value_int, CAST(tags AS string) AS tags_string FROM test_stream_1  ORDER BY id;
SELECT id, name, to_date(created_at) AS created_date, to_start_of_month(created_at) AS month_start, to_datetime('2025-03-12 14:03:00') - created_at AS time_diff FROM test_stream_1  ORDER BY id;
SELECT to_date(created_at) AS created_date, count(*) AS records_count, avg(value) AS average_value FROM test_stream_1  GROUP BY created_date ORDER BY created_date;
SELECT id, CAST(array_map(tag -> (tag, length(tag)), tags) AS array(tuple(string, int32))) AS tag_with_lengths FROM test_stream_1  order by id;
SELECT id, CAST((date_diff('day', to_date(created_at), to_date(to_datetime('2025-03-12 14:03:00'))), name) AS tuple(int32, string)) AS date_diff_and_name FROM test_stream_1  order by id;
SELECT id, CAST(concat(CAST(id AS string), '_', name) AS string) AS combined_id_name FROM test_stream_1  order by id;
SELECT id, name, CAST(array_map(tag -> (tag, 0), tags) AS array(tuple(string, int32))) AS tags_with_index FROM test_stream_1  order by id;
SELECT id, name, value FROM test_stream_1  WHERE value > (SELECT AVG(value) FROM test_stream_1 ) order by id;

-- JOIN
SELECT ts.options, up.profile, COUNT(*) AS active_users_count, SUM(ts.value) AS total_value, array_join(ts.tags) AS tag, COUNT(DISTINCT ts.id) AS users_in_tag, MAX(ts.created_at) AS last_activity FROM test_stream_1 ts JOIN user_profiles up ON ts.id = up.id WHERE ts.is_active = 1 GROUP BY ts.options, up.profile, tag ORDER BY ts.options, tag;
SELECT ts.id, ts.name, ts.options, ts.value, CAST(ts.created_at AS nullable(datetime64(3))) as new_created_at, ts.is_active, up.profile FROM test_stream_1  ts JOIN user_profiles  up ON ts.id = up.id ORDER BY ts.id;

-- subquery
SELECT id, name, value, CAST(to_date(created_at) AS Date) AS created_date FROM test_stream_1  WHERE id IN (SELECT id FROM test_stream_1  WHERE options = 'large' AND value > 50) AND is_active = true ORDER BY value DESC;
SELECT id, name, value, options, to_hour(created_at) AS created_hour FROM test_stream_1  WHERE id IN (SELECT id FROM test_stream_1  WHERE to_start_of_day(created_at) = to_start_of_day(to_date('2025-03-10'))) AND is_active = true ORDER BY created_hour, id;
SELECT id, name, value, to_date(created_at) AS created_date, add_days(to_date(created_at), 7) AS date_plus_seven FROM test_stream_1  WHERE id IN (SELECT id FROM test_stream_1  WHERE value > 50) AND is_active = true ORDER BY created_date;

DROP STREAM IF EXISTS test_stream_1 ;
DROP STREAM IF EXISTS user_profiles ;


DROP STREAM IF EXISTS test_stream_IP;
CREATE STREAM IF NOT EXISTS test_stream_IP 
(
    ipv4_address IPv4,
    ipv6_address IPv6
) ENGINE=MergeTree order by ipv4_address;

INSERT INTO test_stream_IP  (ipv4_address, ipv6_address) VALUES ('192.168.1.1', '2001:0db8:85a3:0000:0000:8a2e:0370:7334');
INSERT INTO test_stream_IP  (ipv4_address, ipv6_address) VALUES ('127.0.0.1', '::1');

-- clickhouse
SELECT ipv4_address, IPv4NumToString(ipv4_address) AS ipv4_address_str FROM test_stream_IP  order by ipv4_address;
SELECT ipv6_address, IPv6NumToString(ipv6_address) AS ipv6_address_str FROM test_stream_IP  order by ipv6_address;
SELECT ipv6_address, isIPv6String(toString(ipv6_address)) AS is_ipv6_valid FROM test_stream_IP  order by ipv6_address;

-- timeplus
SELECT ipv4_address, ipv4_num_to_string(ipv4_address) AS ipv4_address_str FROM test_stream_IP  order by ipv4_address;
SELECT ipv6_address, ipv6_num_to_string(ipv6_address) AS ipv6_address_str FROM test_stream_IP  order by ipv6_address;
SELECT ipv6_address, is_ipv6_string(to_string(ipv6_address)) AS is_ipv6_valid FROM test_stream_IP  order by ipv6_address;

DROP STREAM IF EXISTS test_stream_IP ;
