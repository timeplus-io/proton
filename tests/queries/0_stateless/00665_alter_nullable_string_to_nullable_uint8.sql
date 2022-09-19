DROP STREAM IF EXISTS alter_00665;
create stream alter_00665 (`boolean_false` Nullable(string)) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO alter_00665 (`boolean_false`) VALUES (NULL), (''), ('123');
SELECT * FROM alter_00665;
SELECT * FROM alter_00665 ORDER BY boolean_false NULLS LAST;

ALTER STREAM alter_00665 MODIFY COLUMN `boolean_false` Nullable(uint8);
SELECT * FROM alter_00665;
SELECT * FROM alter_00665 ORDER BY boolean_false NULLS LAST;

DROP STREAM alter_00665;
