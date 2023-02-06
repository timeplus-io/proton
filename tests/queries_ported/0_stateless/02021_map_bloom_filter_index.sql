DROP STREAM IF EXISTS map_test_index_map_keys;
CREATE STREAM map_test_index_map_keys
(
    row_id uint32,
    map map(string, string),
    INDEX map_bloom_filter_keys map_keys(map) TYPE bloom_filter GRANULARITY 1
) Engine=MergeTree() ORDER BY row_id SETTINGS index_granularity = 1;

INSERT INTO map_test_index_map_keys VALUES (0, {'K0':'V0'}), (1, {'K1':'V1'});

SELECT 'map bloom filter map_keys';

SELECT 'Equals with existing key';
SELECT * FROM map_test_index_map_keys WHERE map['K0'] = 'V0' SETTINGS force_data_skipping_indices='map_bloom_filter_keys';
SELECT 'Equals with non existing key';
SELECT * FROM map_test_index_map_keys WHERE map['K2'] = 'V2' SETTINGS force_data_skipping_indices='map_bloom_filter_keys';
SELECT 'Equals with non existing key and default value';
SELECT * FROM map_test_index_map_keys WHERE map['K3'] = '';
SELECT 'Not equals with existing key';
SELECT * FROM map_test_index_map_keys WHERE map['K0'] != 'V0' SETTINGS force_data_skipping_indices='map_bloom_filter_keys';
SELECT 'Not equals with non existing key';
SELECT * FROM map_test_index_map_keys WHERE map['K2'] != 'V2' SETTINGS force_data_skipping_indices='map_bloom_filter_keys';
SELECT 'Not equals with non existing key and default value';
SELECT * FROM map_test_index_map_keys WHERE map['K3'] != '';

SELECT 'IN with existing key';
SELECT * FROM map_test_index_map_keys WHERE map['K0'] IN 'V0' SETTINGS force_data_skipping_indices='map_bloom_filter_keys';
SELECT 'IN with non existing key';
SELECT * FROM map_test_index_map_keys WHERE map['K2'] IN 'V2' SETTINGS force_data_skipping_indices='map_bloom_filter_keys';
SELECT 'IN with non existing key and default value';
SELECT * FROM map_test_index_map_keys WHERE map['K3'] IN '';
SELECT 'NOT IN with existing key';
SELECT * FROM map_test_index_map_keys WHERE map['K0'] NOT IN 'V0' SETTINGS force_data_skipping_indices='map_bloom_filter_keys';
SELECT 'NOT IN with non existing key';
SELECT * FROM map_test_index_map_keys WHERE map['K2'] NOT IN 'V2' SETTINGS force_data_skipping_indices='map_bloom_filter_keys';
SELECT 'NOT IN with non existing key and default value';
SELECT * FROM map_test_index_map_keys WHERE map['K3'] NOT IN '';

SELECT 'map_contains with existing key';
SELECT * FROM map_test_index_map_keys WHERE map_contains(map, 'K0') SETTINGS force_data_skipping_indices='map_bloom_filter_keys';
SELECT 'map_contains with non existing key';
SELECT * FROM map_test_index_map_keys WHERE map_contains(map, 'K2') SETTINGS force_data_skipping_indices='map_bloom_filter_keys';
SELECT 'map_contains with non existing key and default value';
SELECT * FROM map_test_index_map_keys WHERE map_contains(map, '');

SELECT 'Has with existing key';
SELECT * FROM map_test_index_map_keys WHERE has(map, 'K0') SETTINGS force_data_skipping_indices='map_bloom_filter_keys';
SELECT 'Has with non existing key';
SELECT * FROM map_test_index_map_keys WHERE has(map, 'K2') SETTINGS force_data_skipping_indices='map_bloom_filter_keys';
SELECT 'Has with non existing key and default value';
SELECT * FROM map_test_index_map_keys WHERE has(map, '') SETTINGS force_data_skipping_indices='map_bloom_filter_keys';

DROP STREAM map_test_index_map_keys;

DROP STREAM IF EXISTS map_test_index_map_values;
CREATE STREAM map_test_index_map_values
(
    row_id uint32,
    map map(string, string),
    INDEX map_bloom_filter_values map_values(map) TYPE bloom_filter GRANULARITY 1
) Engine=MergeTree() ORDER BY row_id SETTINGS index_granularity = 1;

INSERT INTO map_test_index_map_values VALUES (0, {'K0':'V0'}), (1, {'K1':'V1'});

SELECT 'map bloom filter map_values';

SELECT 'Equals with existing key';
SELECT * FROM map_test_index_map_values WHERE map['K0'] = 'V0' SETTINGS force_data_skipping_indices='map_bloom_filter_values';
SELECT 'Equals with non existing key';
SELECT * FROM map_test_index_map_values WHERE map['K2'] = 'V2' SETTINGS force_data_skipping_indices='map_bloom_filter_values';
SELECT 'Equals with non existing key and default value';
SELECT * FROM map_test_index_map_values WHERE map['K3'] = '';
SELECT 'Not equals with existing key';
SELECT * FROM map_test_index_map_values WHERE map['K0'] != 'V0' SETTINGS force_data_skipping_indices='map_bloom_filter_values';
SELECT 'Not equals with non existing key';
SELECT * FROM map_test_index_map_values WHERE map['K2'] != 'V2' SETTINGS force_data_skipping_indices='map_bloom_filter_values';
SELECT 'Not equals with non existing key and default value';
SELECT * FROM map_test_index_map_values WHERE map['K3'] != '';
SELECT 'IN with existing key';
SELECT * FROM map_test_index_map_values WHERE map['K0'] IN 'V0' SETTINGS force_data_skipping_indices='map_bloom_filter_values';
SELECT 'IN with non existing key';
SELECT * FROM map_test_index_map_values WHERE map['K2'] IN 'V2' SETTINGS force_data_skipping_indices='map_bloom_filter_values';
SELECT 'IN with non existing key and default value';
SELECT * FROM map_test_index_map_values WHERE map['K3'] IN '';
SELECT 'NOT IN with existing key';
SELECT * FROM map_test_index_map_values WHERE map['K0'] NOT IN 'V0' SETTINGS force_data_skipping_indices='map_bloom_filter_values';
SELECT 'NOT IN with non existing key';
SELECT * FROM map_test_index_map_values WHERE map['K2'] NOT IN 'V2' SETTINGS force_data_skipping_indices='map_bloom_filter_values';
SELECT 'NOT IN with non existing key and default value';
SELECT * FROM map_test_index_map_values WHERE map['K3'] NOT IN '';

DROP STREAM map_test_index_map_values;
