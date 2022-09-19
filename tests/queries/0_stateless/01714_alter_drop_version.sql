DROP STREAM IF EXISTS alter_drop_version;

create stream alter_drop_version
(
    `key` uint64,
    `value` string,
    `ver` int8
)
ENGINE = ReplacingMergeTree(ver)
ORDER BY key;

INSERT INTO alter_drop_version VALUES (1, '1', 1);

ALTER STREAM alter_drop_version DROP COLUMN ver; --{serverError 524}
ALTER STREAM alter_drop_version RENAME COLUMN ver TO rev; --{serverError 524}

DETACH TABLE alter_drop_version;

ATTACH TABLE alter_drop_version;

SELECT * FROM alter_drop_version;

DROP STREAM IF EXISTS alter_drop_version;
