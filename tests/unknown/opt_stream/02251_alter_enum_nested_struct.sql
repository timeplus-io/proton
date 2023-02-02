DROP STREAM IF EXISTS alter_enum_array;

CREATE STREAM alter_enum_array(
    Key uint64,
    Value array(enum8('Option1'=1, 'Option2'=2))
)
ENGINE=MergeTree()
ORDER BY tuple();

INSERT INTO alter_enum_array VALUES (1, ['Option2', 'Option1']), (2, ['Option1']);

ALTER STREAM alter_enum_array MODIFY COLUMN Value  array(enum8('Option1'=1, 'Option2'=2, 'Option3'=3)) SETTINGS mutations_sync=2;

INSERT INTO alter_enum_array VALUES (3, ['Option1','Option3']);

SELECT * FROM alter_enum_array ORDER BY Key;

DETACH STREAM alter_enum_array;
ATTACH STREAM alter_enum_array;

SELECT * FROM alter_enum_array ORDER BY Key;

OPTIMIZE STREAM alter_enum_array FINAL;

SELECT count() FROM system.mutations where stream='alter_enum_array' and database=currentDatabase();

DROP STREAM IF EXISTS alter_enum_array;
