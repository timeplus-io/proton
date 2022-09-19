SET allow_experimental_map_type = 1;

DROP STREAM IF EXISTS table_map_with_key_integer;

create stream table_map_with_key_integer (d DATE, m Map(int8, int8))
ENGINE = MergeTree() ORDER BY d;

INSERT INTO table_map_with_key_integer VALUES ('2020-01-01', map(127, 1, 0, 1, -1, 1)) ('2020-01-01', map());

SELECT 'Map(int8, int8)';

SELECT m FROM table_map_with_key_integer;
SELECT m[127], m[1], m[0], m[-1] FROM table_map_with_key_integer;
SELECT m[to_int8(number - 2)] FROM table_map_with_key_integer ARRAY JOIN [0, 1, 2, 3, 4] AS number;

SELECT count() FROM table_map_with_key_integer WHERE m = map();

DROP STREAM IF EXISTS table_map_with_key_integer;

create stream table_map_with_key_integer (d DATE, m Map(int32, uint16))
ENGINE = MergeTree() ORDER BY d;

INSERT INTO table_map_with_key_integer VALUES ('2020-01-01', map(-1, 1, 2147483647, 2, -2147483648, 3));

SELECT 'Map(int32, uint16)';

SELECT m FROM table_map_with_key_integer;
SELECT m[-1], m[2147483647], m[-2147483648] FROM table_map_with_key_integer;
SELECT m[to_int32(number - 2)] FROM table_map_with_key_integer ARRAY JOIN [0, 1, 2, 3, 4] AS number;

DROP STREAM IF EXISTS table_map_with_key_integer;

create stream table_map_with_key_integer (d DATE, m Map(date, int32))
ENGINE = MergeTree() ORDER BY d;

INSERT INTO table_map_with_key_integer VALUES ('2020-01-01', map('2020-01-01', 1, '2020-01-02', 2, '1970-01-02', 3));

SELECT 'Map(date, int32)';

SELECT m FROM table_map_with_key_integer;
SELECT m[to_date('2020-01-01')], m[to_date('2020-01-02')], m[to_date('2020-01-03')] FROM table_map_with_key_integer;
SELECT m[to_date(number)] FROM table_map_with_key_integer ARRAY JOIN [0, 1, 2] AS number;

DROP STREAM IF EXISTS table_map_with_key_integer;

create stream table_map_with_key_integer (d DATE, m Map(UUID, uint16))
ENGINE = MergeTree() ORDER BY d;

INSERT INTO table_map_with_key_integer VALUES ('2020-01-01', map('00001192-0000-4000-8000-000000000001', 1, '00001192-0000-4000-7000-000000000001', 2));

SELECT 'Map(UUID, uint16)';

SELECT m FROM table_map_with_key_integer;
SELECT
    m[toUUID('00001192-0000-4000-6000-000000000001')],
    m[toUUID('00001192-0000-4000-7000-000000000001')],
    m[toUUID('00001192-0000-4000-8000-000000000001')]
FROM table_map_with_key_integer;

SELECT m[257], m[1] FROM table_map_with_key_integer; -- { serverError 43 }

DROP STREAM IF EXISTS table_map_with_key_integer;

create stream table_map_with_key_integer (d DATE, m Map(Int128, string))
ENGINE = MergeTree() ORDER BY d;


INSERT INTO table_map_with_key_integer SELECT '2020-01-01', map(-1, 'a', 0, 'b', to_int128('1234567898765432123456789'), 'c', to_int128('-1234567898765432123456789'), 'd');

SELECT 'Map(Int128, string)';

SELECT m FROM table_map_with_key_integer;
SELECT m[to_int128(-1)], m[to_int128(0)], m[to_int128('1234567898765432123456789')], m[to_int128('-1234567898765432123456789')] FROM table_map_with_key_integer;
SELECT m[to_int128(number - 2)] FROM table_map_with_key_integer ARRAY JOIN [0, 1, 2, 3] AS number;

SELECT m[-1], m[0], m[to_int128('1234567898765432123456789')], m[to_int128('-1234567898765432123456789')] FROM table_map_with_key_integer;
SELECT m[to_uint64(0)], m[to_int64(0)], m[to_uint8(0)], m[to_uint16(0)] FROM table_map_with_key_integer;

DROP STREAM IF EXISTS table_map_with_key_integer;


create stream table_map_with_key_integer (m Map(Float32, string)) ENGINE = MergeTree() ORDER BY tuple(); -- { serverError 36}
create stream table_map_with_key_integer (m Map(Nullable(string), string)) ENGINE = MergeTree() ORDER BY tuple(); -- { serverError 36}
create stream table_map_with_key_integer (m Map(array(uint32), string)) ENGINE = MergeTree() ORDER BY tuple(); -- { serverError 36}
