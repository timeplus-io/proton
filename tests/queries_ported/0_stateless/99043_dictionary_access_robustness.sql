-- Robustness test for JS UDF dictionary access with malformed input.
-- Every case below used to abort the whole server via a fatal V8 check
-- (ToLocalChecked on an empty MaybeLocal) or undefined behavior (casting
-- NaN/negative doubles to uint64): the UDF must instead receive a catchable
-- JS exception, and the server must keep serving lookups afterwards.

CREATE DATABASE IF NOT EXISTS test_dict_robust_;
SELECT sleep(1) FORMAT Null;

CREATE STREAM test_dict_robust_.simple_storage (
    id uint64,
    name string,
    score int32
) PRIMARY KEY (id) SETTINGS mode = 'versioned_kv', flush_threshold_count = 1;

CREATE DICTIONARY test_dict_robust_.simple_dict (
    id uint64,
    name string,
    score int32
)
PRIMARY KEY id
SOURCE(TIMEPLUS(HOST 'localhost' PORT tcp_port() USER 'proton' PASSWORD 'proton@t+' DB 'test_dict_robust_' STREAM 'simple_storage'))
LAYOUT(direct);

INSERT INTO test_dict_robust_.simple_storage (id, name, score) VALUES (1, 'one', 10);

SELECT sleep(3) FORMAT Null;

CREATE OR REPLACE FUNCTION testDictRobustness(v int) RETURNS string LANGUAGE JAVASCRIPT AS $$
function testDictRobustness(vals) {
    const SIMPLE = 'test_dict_robust_.simple_dict';
    const out = [];

    function attempt(label, fn) {
        try {
            const r = fn();
            out.push(label + ': ok ' + JSON.stringify(r));
        } catch (e) {
            out.push(label + ': caught');
        }
    }

    attempt('set null element', () => batchSetValues(SIMPLE, [7], [null]));
    attempt('set undefined element', () => batchSetValues(SIMPLE, [8], [undefined]));
    attempt('set primitive element', () => batchSetValues(SIMPLE, [9], [42]));
    attempt('set sparse values', () => batchSetValues(SIMPLE, [10], [,]));
    attempt('set proxy throwing ownKeys', () => setValue(SIMPLE, 2, new Proxy({}, {ownKeys() { throw new Error('x'); }})));

    attempt('get NaN key', () => getValue(SIMPLE, NaN));
    attempt('get negative key', () => getValue(SIMPLE, -1));
    attempt('get Infinity key', () => getValue(SIMPLE, Infinity));
    attempt('get bad string key', () => getValue(SIMPLE, 'abc'));

    attempt('symbol projection', () => batchGetValues(SIMPLE, [1], [Symbol()]));
    attempt('throwing getter in keys', () => {
        const arr = [1];
        Object.defineProperty(arr, 0, {get() { throw new Error('boom'); }});
        return batchGetValues(SIMPLE, arr);
    });

    attempt('valid lookup after abuse', () => getValue(SIMPLE, 1, ['score']));

    return vals.map(() => out.join('; '));
}
$$;

SELECT sleep(2) FORMAT Null;
SELECT testDictRobustness(1);

DROP FUNCTION testDictRobustness;
DROP DICTIONARY test_dict_robust_.simple_dict;
DROP STREAM test_dict_robust_.simple_storage;
DROP DATABASE test_dict_robust_;
