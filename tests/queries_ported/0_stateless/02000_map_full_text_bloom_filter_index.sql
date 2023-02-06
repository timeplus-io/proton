DROP STREAM IF EXISTS bf_tokenbf_map_keys_test;
DROP STREAM IF EXISTS bf_ngrambf_map_keys_test;

CREATE STREAM bf_tokenbf_map_keys_test
(
    row_id uint32,
    map map(string, string),
    map_fixed map(fixed_string(2), string),
    INDEX map_keys_tokenbf map_keys(map) TYPE tokenbf_v1(256,2,0) GRANULARITY 1,
    INDEX map_fixed_keys_tokenbf map_keys(map_fixed) TYPE ngrambf_v1(4,256,2,0) GRANULARITY 1
) Engine=MergeTree() ORDER BY row_id SETTINGS index_granularity = 1;

INSERT INTO bf_tokenbf_map_keys_test VALUES (0, {'K0':'V0'}, {'K0':'V0'}), (1, {'K1':'V1'}, {'K1':'V1'});

SELECT 'map full text bloom filter tokenbf map_keys';

SELECT 'Equals with existing key';
SELECT * FROM bf_tokenbf_map_keys_test WHERE map['K0'] = 'V0' SETTINGS force_data_skipping_indices='map_keys_tokenbf';
SELECT 'Equals with non existing key';
SELECT * FROM bf_tokenbf_map_keys_test WHERE map['K2'] = 'V2' SETTINGS force_data_skipping_indices='map_keys_tokenbf';
SELECT 'Equals with non existing key and default value';
SELECT * FROM bf_tokenbf_map_keys_test WHERE map['K3'] = '';
SELECT 'Not equals with existing key';
SELECT * FROM bf_tokenbf_map_keys_test WHERE map['K0'] != 'V0' SETTINGS force_data_skipping_indices='map_keys_tokenbf';
SELECT 'Not equals with non existing key';
SELECT * FROM bf_tokenbf_map_keys_test WHERE map['K2'] != 'V2' SETTINGS force_data_skipping_indices='map_keys_tokenbf';
SELECT 'Not equals with non existing key and default value';
SELECT * FROM bf_tokenbf_map_keys_test WHERE map['K3'] != '';

SELECT 'map fixed full text bloom filter tokenbf map_keys';

SELECT 'Equals with existing key';
SELECT * FROM bf_tokenbf_map_keys_test WHERE map_fixed['K0'] = 'V0' SETTINGS force_data_skipping_indices='map_fixed_keys_tokenbf';
SELECT 'Equals with non existing key';
SELECT * FROM bf_tokenbf_map_keys_test WHERE map_fixed['K2'] = 'V2' SETTINGS force_data_skipping_indices='map_fixed_keys_tokenbf';
SELECT 'Equals with non existing key and default value';
SELECT * FROM bf_tokenbf_map_keys_test WHERE map_fixed['K3'] = '';
SELECT 'Not equals with existing key';
SELECT * FROM bf_tokenbf_map_keys_test WHERE map_fixed['K0'] != 'V0' SETTINGS force_data_skipping_indices='map_fixed_keys_tokenbf';
SELECT 'Not equals with non existing key';
SELECT * FROM bf_tokenbf_map_keys_test WHERE map_fixed['K2'] != 'V2' SETTINGS force_data_skipping_indices='map_fixed_keys_tokenbf';
SELECT 'Not equals with non existing key and default value';
SELECT * FROM bf_tokenbf_map_keys_test WHERE map_fixed['K3'] != '';

DROP STREAM bf_tokenbf_map_keys_test;

CREATE STREAM bf_tokenbf_map_values_test
(
    row_id uint32,
    map map(string, string),
    map_fixed map(fixed_string(2), string),
    INDEX map_values_tokenbf map_values(map) TYPE tokenbf_v1(256,2,0) GRANULARITY 1,
    INDEX map_fixed_values_tokenbf map_values(map_fixed) TYPE ngrambf_v1(4,256,2,0) GRANULARITY 1
) Engine=MergeTree() ORDER BY row_id SETTINGS index_granularity = 1;

INSERT INTO bf_tokenbf_map_values_test VALUES (0, {'K0':'V0'}, {'K0':'V0'}), (1, {'K1':'V1'}, {'K1':'V1'});

SELECT 'map full text bloom filter tokenbf map_values';

SELECT 'Equals with existing key';
SELECT * FROM bf_tokenbf_map_values_test WHERE map['K0'] = 'V0' SETTINGS force_data_skipping_indices='map_values_tokenbf';
SELECT 'Equals with non existing key';
SELECT * FROM bf_tokenbf_map_values_test WHERE map['K2'] = 'V2' SETTINGS force_data_skipping_indices='map_values_tokenbf';
SELECT 'Equals with non existing key and default value';
SELECT * FROM bf_tokenbf_map_values_test WHERE map['K3'] = '';
SELECT 'Not equals with existing key';
SELECT * FROM bf_tokenbf_map_values_test WHERE map['K0'] != 'V0' SETTINGS force_data_skipping_indices='map_values_tokenbf';
SELECT 'Not equals with non existing key';
SELECT * FROM bf_tokenbf_map_values_test WHERE map['K2'] != 'V2' SETTINGS force_data_skipping_indices='map_values_tokenbf';
SELECT 'Not equals with non existing key and default value';
SELECT * FROM bf_tokenbf_map_values_test WHERE map['K3'] != '';

SELECT 'map fixed full text bloom filter tokenbf map_keys';

SELECT 'Equals with existing key';
SELECT * FROM bf_tokenbf_map_values_test WHERE map_fixed['K0'] = 'V0' SETTINGS force_data_skipping_indices='map_fixed_values_tokenbf';
SELECT 'Equals with non existing key';
SELECT * FROM bf_tokenbf_map_values_test WHERE map_fixed['K2'] = 'V2' SETTINGS force_data_skipping_indices='map_fixed_values_tokenbf';
SELECT 'Equals with non existing key and default value';
SELECT * FROM bf_tokenbf_map_values_test WHERE map_fixed['K3'] = '';
SELECT 'Not equals with existing key';
SELECT * FROM bf_tokenbf_map_values_test WHERE map_fixed['K0'] != 'V0' SETTINGS force_data_skipping_indices='map_fixed_values_tokenbf';
SELECT 'Not equals with non existing key';
SELECT * FROM bf_tokenbf_map_values_test WHERE map_fixed['K2'] != 'V2' SETTINGS force_data_skipping_indices='map_fixed_values_tokenbf';
SELECT 'Not equals with non existing key and default value';
SELECT * FROM bf_tokenbf_map_values_test WHERE map_fixed['K3'] != '';

DROP STREAM bf_tokenbf_map_values_test;

CREATE STREAM bf_ngrambf_map_keys_test
(
    row_id uint32,
    map map(string, string),
    map_fixed map(fixed_string(2), string),
    INDEX map_keys_ngrambf map_keys(map) TYPE ngrambf_v1(4,256,2,0) GRANULARITY 1,
    INDEX map_fixed_keys_ngrambf map_keys(map_fixed) TYPE ngrambf_v1(4,256,2,0) GRANULARITY 1
) Engine=MergeTree() ORDER BY row_id SETTINGS index_granularity = 1;

INSERT INTO bf_ngrambf_map_keys_test VALUES (0, {'K0':'V0'}, {'K0':'V0'}), (1, {'K1':'V1'}, {'K1':'V1'});

SELECT 'map full text bloom filter ngrambf map_keys';

SELECT 'Equals with existing key';
SELECT * FROM bf_ngrambf_map_keys_test WHERE map['K0'] = 'V0' SETTINGS force_data_skipping_indices='map_keys_ngrambf';
SELECT 'Equals with non existing key';
SELECT * FROM bf_ngrambf_map_keys_test WHERE map['K2'] = 'V2' SETTINGS force_data_skipping_indices='map_keys_ngrambf';
SELECT 'Equals with non existing key and default value';
SELECT * FROM bf_ngrambf_map_keys_test WHERE map['K3'] = '';
SELECT 'Not equals with existing key';
SELECT * FROM bf_ngrambf_map_keys_test WHERE map['K0'] != 'V0' SETTINGS force_data_skipping_indices='map_keys_ngrambf';
SELECT 'Not equals with non existing key';
SELECT * FROM bf_ngrambf_map_keys_test WHERE map['K2'] != 'V2' SETTINGS force_data_skipping_indices='map_keys_ngrambf';
SELECT 'Not equals with non existing key and default value';
SELECT * FROM bf_ngrambf_map_keys_test WHERE map['K3'] != '';

SELECT 'map fixed full text bloom filter ngrambf map_keys';

SELECT 'Equals with existing key';
SELECT * FROM bf_ngrambf_map_keys_test WHERE map_fixed['K0'] = 'V0' SETTINGS force_data_skipping_indices='map_fixed_keys_ngrambf';
SELECT 'Equals with non existing key';
SELECT * FROM bf_ngrambf_map_keys_test WHERE map_fixed['K2'] = 'V2' SETTINGS force_data_skipping_indices='map_fixed_keys_ngrambf';
SELECT 'Equals with non existing key and default value';
SELECT * FROM bf_ngrambf_map_keys_test WHERE map_fixed['K3'] = '';
SELECT 'Not equals with existing key';
SELECT * FROM bf_ngrambf_map_keys_test WHERE map_fixed['K0'] != 'V0' SETTINGS force_data_skipping_indices='map_fixed_keys_ngrambf';
SELECT 'Not equals with non existing key';
SELECT * FROM bf_ngrambf_map_keys_test WHERE map_fixed['K2'] != 'V2' SETTINGS force_data_skipping_indices='map_fixed_keys_ngrambf';
SELECT 'Not equals with non existing key and default value';
SELECT * FROM bf_ngrambf_map_keys_test WHERE map_fixed['K3'] != '';

DROP STREAM bf_ngrambf_map_keys_test;

CREATE STREAM bf_ngrambf_map_values_test
(
    row_id uint32,
    map map(string, string),
    map_fixed map(fixed_string(2), string),
    INDEX map_values_ngrambf map_keys(map) TYPE ngrambf_v1(4,256,2,0) GRANULARITY 1,
    INDEX map_fixed_values_ngrambf map_keys(map_fixed) TYPE ngrambf_v1(4,256,2,0) GRANULARITY 1
) Engine=MergeTree() ORDER BY row_id SETTINGS index_granularity = 1;

INSERT INTO bf_ngrambf_map_values_test VALUES (0, {'K0':'V0'}, {'K0':'V0'}), (1, {'K1':'V1'}, {'K1':'V1'});

SELECT 'map full text bloom filter ngrambf map_values';

SELECT 'Equals with existing key';
SELECT * FROM bf_ngrambf_map_values_test WHERE map['K0'] = 'V0' SETTINGS force_data_skipping_indices='map_values_ngrambf';
SELECT 'Equals with non existing key';
SELECT * FROM bf_ngrambf_map_values_test WHERE map['K2'] = 'V2' SETTINGS force_data_skipping_indices='map_values_ngrambf';
SELECT 'Equals with non existing key and default value';
SELECT * FROM bf_ngrambf_map_values_test WHERE map['K3'] = '';
SELECT 'Not equals with existing key';
SELECT * FROM bf_ngrambf_map_values_test WHERE map['K0'] != 'V0' SETTINGS force_data_skipping_indices='map_values_ngrambf';
SELECT 'Not equals with non existing key';
SELECT * FROM bf_ngrambf_map_values_test WHERE map['K2'] != 'V2' SETTINGS force_data_skipping_indices='map_values_ngrambf';
SELECT 'Not equals with non existing key and default value';
SELECT * FROM bf_ngrambf_map_values_test WHERE map['K3'] != '';

SELECT 'map fixed full text bloom filter ngrambf map_keys';

SELECT 'Equals with existing key';
SELECT * FROM bf_ngrambf_map_values_test WHERE map_fixed['K0'] = 'V0' SETTINGS force_data_skipping_indices='map_fixed_values_ngrambf';
SELECT 'Equals with non existing key';
SELECT * FROM bf_ngrambf_map_values_test WHERE map_fixed['K2'] = 'V2' SETTINGS force_data_skipping_indices='map_fixed_values_ngrambf';
SELECT 'Equals with non existing key and default value';
SELECT * FROM bf_ngrambf_map_values_test WHERE map_fixed['K3'] = '';
SELECT 'Not equals with existing key';
SELECT * FROM bf_ngrambf_map_values_test WHERE map_fixed['K0'] != 'V0' SETTINGS force_data_skipping_indices='map_fixed_values_ngrambf';
SELECT 'Not equals with non existing key';
SELECT * FROM bf_ngrambf_map_values_test WHERE map_fixed['K2'] != 'V2' SETTINGS force_data_skipping_indices='map_fixed_values_ngrambf';
SELECT 'Not equals with non existing key and default value';
SELECT * FROM bf_ngrambf_map_values_test WHERE map_fixed['K3'] != '';

DROP STREAM bf_ngrambf_map_values_test;
