SELECT '00000000-0000-01f8-9cb8-cb1b82fb3900' AS str, toUUID(str);
SELECT to_fixed_string('00000000-0000-02f8-9cb8-cb1b82fb3900', 36) AS str, toUUID(str);

SELECT '00000000-0000-03f8-9cb8-cb1b82fb3900' AS str, CAST(str, 'UUID');
SELECT to_fixed_string('00000000-0000-04f8-9cb8-cb1b82fb3900', 36) AS str, CAST(str, 'UUID');

DROP STREAM IF EXISTS uuid;
create stream IF NOT EXISTS uuid
(
    created_at DateTime,
    id0 string,
    id1 FixedString(36)
)
ENGINE = MergeTree
PARTITION BY to_date(created_at)
ORDER BY (created_at);

INSERT INTO uuid VALUES ('2018-01-01 01:02:03', '00000000-0000-05f8-9cb8-cb1b82fb3900', '00000000-0000-06f8-9cb8-cb1b82fb3900');

ALTER STREAM uuid MODIFY COLUMN id0 UUID;
ALTER STREAM uuid MODIFY COLUMN id1 UUID;

SELECT id0, id1 FROM uuid;
SELECT to_type_name(id0), to_type_name(id1) FROM uuid;

DROP STREAM uuid;

-- with UUID in key

create stream IF NOT EXISTS uuid
(
    created_at DateTime,
    id0 string,
    id1 FixedString(36)
)
ENGINE = MergeTree
PARTITION BY to_date(created_at)
ORDER BY (created_at, id0, id1);

SET send_logs_level = 'fatal';

ALTER STREAM uuid MODIFY COLUMN id0 UUID; -- { serverError 524 }
ALTER STREAM uuid MODIFY COLUMN id1 UUID; -- { serverError 524 }

DROP STREAM uuid;
