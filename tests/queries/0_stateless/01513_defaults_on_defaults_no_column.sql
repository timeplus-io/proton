DROP STREAM IF EXISTS defaults_on_defaults;
create stream defaults_on_defaults (
    key uint64
)
ENGINE = MergeTree()
ORDER BY tuple();

INSERT INTO defaults_on_defaults values (1);

ALTER STREAM defaults_on_defaults ADD COLUMN `Arr.C1` array(uint32) DEFAULT emptyArrayUInt32();

ALTER STREAM defaults_on_defaults ADD COLUMN `Arr.C2` array(uint32) DEFAULT arrayResize(emptyArrayUInt32(), length(Arr.C1));

ALTER STREAM defaults_on_defaults ADD COLUMN `Arr.C3` array(uint32) ALIAS arrayResize(emptyArrayUInt32(), length(Arr.C2));

SELECT 1 from defaults_on_defaults where length(`Arr.C2`) = 0;

SELECT 1 from defaults_on_defaults where length(`Arr.C3`) = 0;

ALTER STREAM defaults_on_defaults ADD COLUMN `Arr.C4` array(uint32) DEFAULT arrayResize(emptyArrayUInt32(), length(Arr.C3));

SELECT 1 from defaults_on_defaults where length(`Arr.C4`) = 0;

ALTER STREAM defaults_on_defaults ADD COLUMN `ArrLen` uint64 DEFAULT length(Arr.C4);

SELECT 1 from defaults_on_defaults where ArrLen = 0;

SELECT * from defaults_on_defaults where ArrLen = 0;

SHOW create stream defaults_on_defaults;

OPTIMIZE STREAM defaults_on_defaults FINAL;

SELECT 1 from defaults_on_defaults where length(`Arr.C4`) = 0;

DROP STREAM IF EXISTS defaults_on_defaults;
