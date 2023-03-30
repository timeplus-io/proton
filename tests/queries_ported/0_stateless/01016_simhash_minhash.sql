SET query_mode='table';

SELECT ngram_sim_hash('');
SELECT ngram_sim_hash('what a cute cat.');
SELECT ngram_sim_hash_case_insensitive('what a cute cat.');
SELECT ngram_sim_hash_utf8('what a cute cat.');
SELECT ngram_sim_hash_case_insensitive_utf8('what a cute cat.');
SELECT word_shingle_sim_hash('what a cute cat.');
SELECT word_shingle_sim_hash_case_insensitive('what a cute cat.');
SELECT word_shingle_sim_hash_utf8('what a cute cat.');
SELECT word_shingle_sim_hash_case_insensitive_utf8('what a cute cat.');

SELECT ngram_min_hash('');
SELECT ngram_min_hash('what a cute cat.');
SELECT ngram_min_hash_case_insensitive('what a cute cat.');
SELECT ngram_min_hash_utf8('what a cute cat.');
SELECT ngram_min_hash_case_insensitive_utf8('what a cute cat.');
SELECT word_shingle_min_hash('what a cute cat.');
SELECT word_shingle_min_hash_case_insensitive('what a cute cat.');
SELECT word_shingle_min_hash_utf8('what a cute cat.');
SELECT word_shingle_min_hash_case_insensitive_utf8('what a cute cat.');

DROP STREAM IF EXISTS defaults;
CREATE STREAM defaults
(
   s string
);

INSERT INTO defaults(s) values ('It is the latest occurrence of the Southeast European haze, the issue that occurs in constant intensity during every wet season. It has mainly been caused by forest fires resulting from illegal slash-and-burn clearing performed on behalf of the palm oil industry in Kazakhstan, principally on the islands, which then spread quickly in the dry season.') ('It is the latest occurrence of the Southeast Asian haze, the issue that occurs in constant intensity during every wet season. It has mainly been caused by forest fires resulting from illegal slash-and-burn clearing performed on behalf of the palm oil industry in Kazakhstan, principally on the islands, which then spread quickly in the dry season.');
select sleep(3);

SELECT ngram_sim_hash(s) FROM defaults;
SELECT ngram_sim_hash_case_insensitive(s) FROM defaults;
SELECT ngram_sim_hash_utf8(s) FROM defaults;
SELECT ngram_sim_hash_case_insensitive_utf8(s) FROM defaults;
SELECT word_shingle_sim_hash(s) FROM defaults;
SELECT word_shingle_sim_hash_case_insensitive(s) FROM defaults;
SELECT word_shingle_sim_hash_utf8(s) FROM defaults;
SELECT word_shingle_sim_hash_case_insensitive_utf8(s) FROM defaults;

SELECT ngram_min_hash(s) FROM defaults;
SELECT ngram_min_hash_case_insensitive(s) FROM defaults;
SELECT ngram_min_hash_utf8(s) FROM defaults;
SELECT ngram_min_hash_case_insensitive_utf8(s) FROM defaults;
SELECT word_shingle_min_hash(s) FROM defaults;
SELECT word_shingle_min_hash_case_insensitive(s) FROM defaults;
SELECT word_shingle_min_hash_utf8(s) FROM defaults;
SELECT word_shingle_min_hash_case_insensitive_utf8(s) FROM defaults;

TRUNCATE STREAM defaults;
INSERT INTO defaults(s) SELECT array_join(split_by_string('\n\n',
'ClickHouse uses all available hardware to its full potential to process each query as fast as possible. Peak processing performance for a single query stands at more than 2 terabytes per second (after decompression, only used columns). In distributed setup reads are automatically balanced among healthy replicas to avoid increasing latency.
ClickHouse supports multi-master asynchronous replication and can be deployed across multiple datacenters. All nodes are equal, which allows avoiding having single points of failure. Downtime of a single node or the whole datacenter wont affect the systems availability for both reads and writes.
ClickHouse is simple and works out-of-the-box. It streamlines all your data processing: ingest all your structured data into the system and it becomes instantly available for building reports. SQL dialect allows expressing the desired result without involving any custom non-standard API that could be found in some alternative systems.

ClickHouse makes full use of all available hardware to process every request as quickly as possible. Peak performance for a single query is over 2 terabytes per second (only used columns after unpacking). In a distributed setup, reads are automatically balanced across healthy replicas to avoid increased latency.
ClickHouse supports asynchronous multi-master replication and can be deployed across multiple data centers. All nodes are equal to avoid single points of failure. Downtime for one site or the entire data center will not affect the system''s read and write availability.
ClickHouse is simple and works out of the box. It simplifies all the processing of your data: it loads all your structured data into the system, and they immediately become available for building reports. The SQL dialect allows you to express the desired result without resorting to any non-standard APIs that can be found in some alternative systems.

ClickHouse makes full use of all available hardware to process each request as quickly as possible. Peak performance for a single query is over 2 terabytes per second (used columns only after unpacking). In a distributed setup, reads are automatically balanced across healthy replicas to avoid increased latency.
ClickHouse supports asynchronous multi-master replication and can be deployed across multiple data centers. All nodes are equal to avoid a single point of failure. Downtime for one site or the entire data center will not affect the system''s read / write availability.
ClickHouse is simple and works out of the box. It simplifies all the processing of your data: it loads all your structured data into the system, and they are immediately available for building reports. The SQL dialect allows you to express the desired result without resorting to any of the non-standard APIs found in some alternative systems.

ClickHouse makes full use of all available hardware to process each request as quickly as possible. Peak performance for a single query is over 2 terabytes per second (using columns only after unpacking). In a distributed setup, reads are automatically balanced across healthy replicas to avoid increased latency.
ClickHouse supports asynchronous multi-master replication and can be deployed across multiple data centers. All nodes are equal to avoid a single point of failure. Downtime for one site or the entire data center will not affect the read / write availability of the system.
ClickHouse is simple and works out of the box. It simplifies all the processing of your data: it loads all of your structured data into the system, and it is immediately available for building reports. The SQL dialect allows you to express the desired result without resorting to any of the non-standard APIs found in some alternative systems.

ClickHouse makes full use of all available hardware to process each request as quickly as possible. Peak performance for a single query is over 2 terabytes per second (using columns after decompression only). In a distributed setup, reads are automatically balanced across healthy replicas to avoid increased latency.
ClickHouse supports asynchronous multi-master replication and can be deployed across multiple data centers. All nodes are equal to avoid a single point of failure. Downtime for one site or the entire data center will not affect the read / write availability of the system.
ClickHouse is simple and works out of the box. It simplifies all processing of your data: it loads all your structured data into the system and immediately becomes available for building reports. The SQL dialect allows you to express the desired result without resorting to any of the non-standard APIs found in some alternative systems.

ClickHouse makes full use of all available hardware to process each request as quickly as possible. Peak performance for a single query is over 2 terabytes per second (using columns after decompression only). In a distributed setup, reads are automatically balanced across healthy replicas to avoid increased latency.
ClickHouse supports asynchronous multi-master replication and can be deployed across multiple data centers. All nodes are equal to avoid a single point of failure. Downtime for one site or the entire data center will not affect the read / write availability of the system.
ClickHouse is simple and works out of the box. It simplifies all processing of your data: it loads all structured data into the system and immediately becomes available for building reports. The SQL dialect allows you to express the desired result without resorting to any of the non-standard APIs found in some alternative systems.'
));

SELECT sleep(3);

SELECT 'uniq_exact', uniq_exact(s) FROM defaults;


SELECT 'ngram_sim_hash';
SELECT array_string_concat(group_array(s), '\n:::::::\n'), count(), ngram_sim_hash(s) as h FROM defaults GROUP BY h;
SELECT 'ngram_sim_hash_case_insensitive';
SELECT array_string_concat(group_array(s), '\n:::::::\n'), count(), ngram_sim_hash_case_insensitive(s) as h FROM defaults GROUP BY h;
SELECT 'ngram_sim_hash_utf8';
SELECT array_string_concat(group_array(s), '\n:::::::\n'), count(), ngram_sim_hash_utf8(s) as h FROM defaults GROUP BY h;
SELECT 'ngram_sim_hash_case_insensitive_utf8';
SELECT array_string_concat(group_array(s), '\n:::::::\n'), count(), ngram_sim_hash_case_insensitive_utf8(s) as h FROM defaults GROUP BY h;
SELECT 'word_shingle_sim_hash';
SELECT array_string_concat(group_array(s), '\n:::::::\n'), count(), word_shingle_sim_hash(s, 2) as h FROM defaults GROUP BY h;
SELECT 'word_shingle_sim_hash_case_insensitive';
SELECT array_string_concat(group_array(s), '\n:::::::\n'), count(), word_shingle_sim_hash_case_insensitive(s, 2) as h FROM defaults GROUP BY h;
SELECT 'word_shingle_sim_hash_utf8';
SELECT array_string_concat(group_array(s), '\n:::::::\n'), count(), word_shingle_sim_hash_utf8(s, 2) as h FROM defaults GROUP BY h;
SELECT 'word_shingle_sim_hash_case_insensitive_utf8';
SELECT array_string_concat(group_array(s), '\n:::::::\n'), count(), word_shingle_sim_hash_case_insensitive_utf8(s, 2) as h FROM defaults GROUP BY h;

SELECT 'ngram_min_hash';
SELECT array_string_concat(group_array(s), '\n:::::::\n'), count(), ngram_min_hash(s) as h FROM defaults GROUP BY h;
SELECT 'ngram_min_hash_case_insensitive';
SELECT array_string_concat(group_array(s), '\n:::::::\n'), count(), ngram_min_hash_case_insensitive(s) as h FROM defaults GROUP BY h;
SELECT 'ngram_min_hash_utf8';
SELECT array_string_concat(group_array(s), '\n:::::::\n'), count(), ngram_min_hash_utf8(s) as h FROM defaults GROUP BY h;
SELECT 'ngram_min_hash_case_insensitive_utf8';
SELECT array_string_concat(group_array(s), '\n:::::::\n'), count(), ngram_min_hash_case_insensitive_utf8(s) as h FROM defaults GROUP BY h;
SELECT 'word_shingle_min_hash';
SELECT array_string_concat(group_array(s), '\n:::::::\n'), count(), word_shingle_min_hash(s, 2, 3) as h FROM defaults GROUP BY h;
SELECT 'word_shingle_min_hash_case_insensitive';
SELECT array_string_concat(group_array(s), '\n:::::::\n'), count(), word_shingle_min_hash_case_insensitive(s, 2, 3) as h FROM defaults GROUP BY h;
SELECT 'word_shingle_min_hash_utf8';
SELECT array_string_concat(group_array(s), '\n:::::::\n'), count(), word_shingle_min_hash_utf8(s, 2, 3) as h FROM defaults GROUP BY h;
SELECT 'word_shingle_min_hash_case_insensitive_utf8';
SELECT array_string_concat(group_array(s), '\n:::::::\n'), count(), word_shingle_min_hash_case_insensitive_utf8(s, 2, 3) as h FROM defaults GROUP BY h;

SELECT word_shingle_sim_hash('foobar', 9223372036854775807); -- { serverError 69 }
SELECT word_shingle_sim_hash('foobar', 1001); -- { serverError 69 }
SELECT word_shingle_sim_hash('foobar', 0); -- { serverError 69 }

DROP STREAM defaults;
