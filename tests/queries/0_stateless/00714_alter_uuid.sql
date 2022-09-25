SELECT '00000000-0000-01f8-9cb8-cb1b82fb3900' AS str, to_uuid(str);
SELECT to_fixed_string('00000000-0000-02f8-9cb8-cb1b82fb3900', 36) AS str, to_uuid(str);

SELECT '00000000-0000-03f8-9cb8-cb1b82fb3900' AS str, CAST(str, 'uuid');
SELECT to_fixed_string('00000000-0000-04f8-9cb8-cb1b82fb3900', 36) AS str, CAST(str, 'uuid');

DROP STREAM IF EXISTS uuid;
create stream IF NOT EXISTS uuid
(
    created_at datetime,
    id0 string,
    id1 fixed_string(36)
)
ENGINE = MergeTree
PARTITION BY to_date(created_at)
ORDER BY (created_at);

INSERT INTO uuid VALUES ('2018-01-01 01:02:03', '00000000-0000-05f8-9cb8-cb1b82fb3900', '00000000-0000-06f8-9cb8-cb1b82fb3900');

ALTER STREAM uuid MODIFY COLUMN id0 uuid;
ALTER STREAM uuid MODIFY COLUMN id1 uuid;

SELECT id0, id1 FROM uuid;
SELECT to_type_name(id0), to_type_name(id1) FROM uuid;

DROP STREAM uuid;

-- with uuid in key

create stream IF NOT EXISTS uuid
(
    created_at datetime,
    id0 string,
    id1 fixed_string(36)
)
ENGINE = MergeTree
PARTITION BY to_date(created_at)
ORDER BY (created_at, id0, id1);

 

ALTER STREAM uuid MODIFY COLUMN id0 uuid; -- { serverError 524 }
ALTER STREAM uuid MODIFY COLUMN id1 uuid; -- { serverError 524 }

DROP STREAM uuid;
