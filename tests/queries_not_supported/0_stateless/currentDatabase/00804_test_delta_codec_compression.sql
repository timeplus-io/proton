-- Tags: no-parallel

SET send_logs_level = 'fatal';
SET joined_subquery_requires_alias = 0;

DROP STREAM IF EXISTS delta_codec_synthetic;
DROP STREAM IF EXISTS default_codec_synthetic;

create stream delta_codec_synthetic
(
    id uint64 Codec(Delta, ZSTD(3))
) ENGINE MergeTree() ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;

create stream default_codec_synthetic
(
    id uint64 Codec(ZSTD(3))
) ENGINE MergeTree() ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO delta_codec_synthetic SELECT number FROM system.numbers LIMIT 5000000;
INSERT INTO default_codec_synthetic SELECT number FROM system.numbers LIMIT 5000000;

OPTIMIZE STREAM delta_codec_synthetic FINAL;
OPTIMIZE STREAM default_codec_synthetic FINAL;

SELECT
    floor(big_size / small_size) AS ratio
FROM
    (SELECT 1 AS key, sum(bytes_on_disk) AS small_size FROM system.parts WHERE database == currentDatabase() and table == 'delta_codec_synthetic' and active)
INNER JOIN
    (SELECT 1 AS key, sum(bytes_on_disk) as big_size FROM system.parts WHERE database == currentDatabase() and table == 'default_codec_synthetic' and active)
USING(key);

SELECT
    small_hash == big_hash
FROM
    (SELECT 1 AS key, sum(cityHash64(*)) AS small_hash FROM delta_codec_synthetic)
INNER JOIN
    (SELECT 1 AS key, sum(cityHash64(*)) AS big_hash FROM default_codec_synthetic)
USING(key);

DROP STREAM IF EXISTS delta_codec_synthetic;
DROP STREAM IF EXISTS default_codec_synthetic;

DROP STREAM IF EXISTS delta_codec_float;
DROP STREAM IF EXISTS default_codec_float;

create stream delta_codec_float
(
    id float64 Codec(Delta, LZ4HC)
) ENGINE MergeTree() ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;

create stream default_codec_float
(
    id float64 Codec(LZ4HC)
) ENGINE MergeTree() ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO delta_codec_float SELECT number FROM numbers(1547510400, 500000) WHERE number % 3 == 0 OR number % 5 == 0 OR number % 7 == 0 OR number % 11 == 0;
INSERT INTO default_codec_float SELECT * from delta_codec_float;

OPTIMIZE STREAM delta_codec_float FINAL;
OPTIMIZE STREAM default_codec_float FINAL;

SELECT
    floor(big_size / small_size) as ratio
FROM
    (SELECT 1 AS key, sum(bytes_on_disk) AS small_size FROM system.parts WHERE database = currentDatabase() and table = 'delta_codec_float' and active)
INNER JOIN
    (SELECT 1 AS key, sum(bytes_on_disk) as big_size FROM system.parts WHERE database = currentDatabase() and table = 'default_codec_float' and active) USING(key);

SELECT
    small_hash == big_hash
FROM
    (SELECT 1 AS key, sum(cityHash64(*)) AS small_hash FROM delta_codec_float)
INNER JOIN
    (SELECT 1 AS key, sum(cityHash64(*)) AS big_hash FROM default_codec_float)
USING(key);

DROP STREAM IF EXISTS delta_codec_float;
DROP STREAM IF EXISTS default_codec_float;


DROP STREAM IF EXISTS delta_codec_string;
DROP STREAM IF EXISTS default_codec_string;

create stream delta_codec_string
(
    id float64 Codec(Delta, LZ4)
) ENGINE MergeTree() ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;

create stream default_codec_string
(
    id float64 Codec(LZ4)
) ENGINE MergeTree() ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO delta_codec_string SELECT concat(to_string(number), to_string(number % 100)) FROM numbers(1547510400, 500000);
INSERT INTO default_codec_string SELECT * from delta_codec_string;

OPTIMIZE STREAM delta_codec_string FINAL;
OPTIMIZE STREAM default_codec_string FINAL;

SELECT
    floor(big_size / small_size) as ratio
FROM
    (SELECT 1 AS key, sum(bytes_on_disk) AS small_size FROM system.parts WHERE database = currentDatabase() and table = 'delta_codec_string' and active)
INNER JOIN
    (SELECT 1 AS key, sum(bytes_on_disk) as big_size FROM system.parts WHERE database = currentDatabase() and table = 'default_codec_string' and active) USING(key);

SELECT
    small_hash == big_hash
FROM
    (SELECT 1 AS key, sum(cityHash64(*)) AS small_hash FROM delta_codec_string)
INNER JOIN
    (SELECT 1 AS key, sum(cityHash64(*)) AS big_hash FROM default_codec_string)
USING(key);

DROP STREAM IF EXISTS delta_codec_string;
DROP STREAM IF EXISTS default_codec_string;
