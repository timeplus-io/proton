SET group_by_two_level_threshold_bytes = 1;
SET group_by_two_level_threshold = 1;

SELECT group_array(DISTINCT to_string(number % 10)) FROM numbers_mt(50000) 
    GROUP BY number ORDER BY number LIMIT 10
    SETTINGS max_threads = 2, max_block_size = 2000;

DROP STREAM IF EXISTS dictinct_two_level;

create stream dictinct_two_level (
    time DateTime64(3),
    domain string,
    subdomain string
) ENGINE = MergeTree ORDER BY time;

INSERT INTO dictinct_two_level SELECT 1546300800000, 'test.com', concat('foo', to_string(number % 10000)) from numbers(10000);
INSERT INTO dictinct_two_level SELECT 1546300800000, concat('test.com', to_string(number / 10000)) , concat('foo', to_string(number % 10000)) from numbers(10000);

SELECT
    domain, groupArraySample(5, 11111)(DISTINCT subdomain) AS example_subdomains
FROM dictinct_two_level
GROUP BY domain ORDER BY domain, example_subdomains
LIMIT 10;

DROP STREAM IF EXISTS dictinct_two_level;
