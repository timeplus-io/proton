-- Tags: distributed

DROP STREAM IF EXISTS test_distributed;
DROP STREAM IF EXISTS test_local;

SET prefer_localhost_replica = 1;

-- https://github.com/ClickHouse/ClickHouse/issues/36279
CREATE STREAM test_local (text string, text2 string) ENGINE = MergeTree() ORDER BY text;
CREATE STREAM test_distributed (text string, text2 string) ENGINE = Distributed('test_shard_localhost', current_database(), test_local);
INSERT INTO test_distributed SELECT randomString(100) AS text, randomString(100) AS text2 FROM system.numbers LIMIT 1;

SET joined_subquery_requires_alias = 0;

SELECT count() AS count
FROM test_distributed
INNER JOIN
(
    SELECT text
    FROM test_distributed
    WHERE (text ILIKE '%text-for-search%') AND (text2 ILIKE '%text-for-search%')
) USING (text)
WHERE (text ILIKE '%text-for-search%') AND (text2 ILIKE '%text-for-search%')
;

DROP STREAM IF EXISTS test_distributed;
DROP STREAM IF EXISTS test_local;

DROP STREAM IF EXISTS user_local;
DROP STREAM IF EXISTS user_all;
DROP STREAM IF EXISTS event;

-- https://github.com/ClickHouse/ClickHouse/issues/36300
CREATE STREAM user_local ( id int64, name string, age int32 )
ENGINE = MergeTree ORDER BY name;

CREATE STREAM user_all ( id int64, name string, age int32 )
ENGINE = Distributed('test_shard_localhost', current_database(), user_local, rand());

CREATE STREAM event ( id int64, user_id int64, content string, created_time DateTime )
ENGINE = MergeTree ORDER BY user_id;

INSERT INTO user_local (id, name, age) VALUES (1, 'aaa', 21);
INSERT INTO event (id, user_id, content, created_time) VALUES(1, 1, 'hello', '2022-01-05 12:00:00');

SELECT
  u.name user_name,
  20 AS age_group
FROM user_all u
LEFT JOIN event e ON u.id = e.user_id
WHERE (u.age >= 20 AND u.age < 30)
AND e.created_time > '2022-01-01';

DROP STREAM IF EXISTS user_local;
DROP STREAM IF EXISTS user_all;
DROP STREAM IF EXISTS event;
