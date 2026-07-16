-- Regression test for JS UDF dictionary access with missing keys:
-- 1. default behavior preserves the legacy default-filled object for missing keys
-- 2. javascript_udf_getvalue_null_on_missing_key=1 returns null for missing keys
-- 3. batchGetValues keeps result rows aligned with input keys under both modes
-- 4. column projection returns requested columns by name

CREATE DATABASE IF NOT EXISTS test_dict_missing_;
SELECT sleep(1) FORMAT Null;

CREATE STREAM test_dict_missing_.simple_storage (
    id uint64,
    name string,
    score int32
) PRIMARY KEY (id) SETTINGS mode = 'versioned_kv', flush_threshold_count = 1;

CREATE STREAM test_dict_missing_.complex_storage (
    k1 string,
    k2 string,
    name string,
    score int32
) PRIMARY KEY (k1, k2) SETTINGS mode = 'versioned_kv', flush_threshold_count = 1;

CREATE DICTIONARY test_dict_missing_.simple_dict (
    id uint64,
    name string,
    score int32
)
PRIMARY KEY id
SOURCE(TIMEPLUS(HOST 'localhost' PORT tcp_port() USER 'proton' PASSWORD 'proton@t+' DB 'test_dict_missing_' STREAM 'simple_storage'))
LAYOUT(direct);

CREATE DICTIONARY test_dict_missing_.complex_dict (
    k1 string,
    k2 string,
    name string,
    score int32
)
PRIMARY KEY (k1, k2)
SOURCE(TIMEPLUS(HOST 'localhost' PORT tcp_port() USER 'proton' PASSWORD 'proton@t+' DB 'test_dict_missing_' STREAM 'complex_storage'))
LAYOUT(complex_key_direct);

INSERT INTO test_dict_missing_.simple_storage (id, name, score) VALUES (1, 'one', 10), (2, 'two', 20), (3, 'three', 30);
INSERT INTO test_dict_missing_.complex_storage (k1, k2, name, score) VALUES ('a', 'x', 'ax', 1), ('b', 'y', 'by', 2);

SELECT sleep(3) FORMAT Null;

CREATE OR REPLACE FUNCTION testDictMissingKeys(v int) RETURNS string LANGUAGE JAVASCRIPT AS $$
function testDictMissingKeys(vals) {
    const SIMPLE = 'test_dict_missing_.simple_dict';
    const COMPLEX = 'test_dict_missing_.complex_dict';
    const out = [];

    function kind(row) {
        return row === null ? 'null' : 'object';
    }

    function missingShape(row) {
        if (row === null) {
            return 'null';
        }

        return 'object name=' + JSON.stringify(row.name) + ' score=' + row.score;
    }

    out.push('get existing: ' + JSON.stringify(getValue(SIMPLE, 2)));

    out.push('get missing simple: ' + missingShape(getValue(SIMPLE, 999)));
    out.push('get missing complex: ' + missingShape(getValue(COMPLEX, {k1: 'no', k2: 'pe'})));

    const simpleBatch = batchGetValues(SIMPLE, [1, 999, 2, 3]);
    out.push('batch simple kinds: ' + simpleBatch.map(kind).join(','));
    out.push('batch simple values: ' + JSON.stringify(simpleBatch.map(row => row === null ? null : row.score)));

    const complexBatch = batchGetValues(COMPLEX, [
        {k1: 'a', k2: 'x'},
        {k1: 'no', k2: 'pe'},
        {k1: 'b', k2: 'y'}
    ]);
    out.push('batch complex kinds: ' + complexBatch.map(kind).join(','));
    out.push('batch complex values: ' + JSON.stringify(complexBatch.map(row => row === null ? null : row.score)));

    out.push('proj attr only: ' + JSON.stringify(getValue(SIMPLE, 2, ['name'])));
    out.push('proj key+attr: ' + JSON.stringify(getValue(SIMPLE, 2, ['id', 'score'])));

    const projectedBatch = batchGetValues(SIMPLE, [3, 999, 1], ['score']);
    out.push('batch proj kinds: ' + projectedBatch.map(kind).join(','));
    out.push('batch proj values: ' + JSON.stringify(projectedBatch.map(row => row === null ? null : row.score)));

    return vals.map(v => out.join('; '));
}
$$;

SELECT sleep(2) FORMAT Null;
SELECT testDictMissingKeys(1);
SELECT testDictMissingKeys(1) SETTINGS javascript_udf_getvalue_null_on_missing_key=1;

CREATE OR REPLACE AGGREGATE FUNCTION testDictMissingKeysAggr(v uint64, _tp_delta int8) RETURNS string LANGUAGE JAVASCRIPT AS $$
{
    initialize: function() {
        this.out = [];
    },
    process: function(vals) {
        const SIMPLE = 'test_dict_missing_.simple_dict';

        function kind(row) {
            return row === null ? 'null' : 'object';
        }

        function missingShape(row) {
            if (row === null) {
                return 'null';
            }

            return 'object name=' + JSON.stringify(row.name) + ' score=' + row.score;
        }

        if (this.out.length === 0) {
            const simpleBatch = batchGetValues(SIMPLE, [1, 999, 2], ['score']);
            this.out.push('uda missing simple: ' + missingShape(getValue(SIMPLE, 999)));
            this.out.push('uda batch kinds: ' + simpleBatch.map(kind).join(','));
            this.out.push('uda batch values: ' + JSON.stringify(simpleBatch.map(row => row === null ? null : row.score)));
        }
    },
    finalize: function() {
        return this.out.join('; ');
    },
    serialize: function() {
        return JSON.stringify({out: this.out});
    },
    deserialize: function(state_str) {
        const state = JSON.parse(state_str);
        this.out = state.out;
    },
    merge: function(state_str) {
        const state = JSON.parse(state_str);
        this.out = this.out.concat(state.out);
    }
}
$$;

SELECT testDictMissingKeysAggr(number) FROM numbers(1);
SELECT testDictMissingKeysAggr(number) FROM numbers(1) SETTINGS javascript_udf_getvalue_null_on_missing_key=1;

DROP FUNCTION testDictMissingKeys;
DROP FUNCTION testDictMissingKeysAggr;
DROP DICTIONARY test_dict_missing_.simple_dict;
DROP DICTIONARY test_dict_missing_.complex_dict;
DROP STREAM test_dict_missing_.simple_storage;
DROP STREAM test_dict_missing_.complex_storage;
DROP DATABASE test_dict_missing_;
