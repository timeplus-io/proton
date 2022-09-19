-- Tags: no-parallel

DROP STREAM IF EXISTS tbl;
create stream tbl(a string, b uint32, c float64, d int64, e uint8) ENGINE=MergeTree ORDER BY tuple();
INSERT INTO tbl SELECT number, number * 2, number * 3, number * 4, number * 5 FROM system.numbers LIMIT 10;

SET mutations_sync = 1;

-- Change the types of columns by adding a temporary column and updating and dropping.
-- Alters should be executed in sequential order.
ALTER STREAM tbl ADD COLUMN xi int64;
ALTER STREAM tbl UPDATE xi = a WHERE 1;
ALTER STREAM tbl DROP COLUMN a;
ALTER STREAM tbl ADD COLUMN a int64;
ALTER STREAM tbl UPDATE a = xi WHERE 1;
ALTER STREAM tbl DROP COLUMN xi;

ALTER STREAM tbl ADD COLUMN xi string;
ALTER STREAM tbl UPDATE xi = b WHERE 1;
ALTER STREAM tbl DROP COLUMN b;
ALTER STREAM tbl ADD COLUMN b string;
ALTER STREAM tbl UPDATE b = xi WHERE 1;
ALTER STREAM tbl DROP COLUMN xi;

ALTER STREAM tbl ADD COLUMN xi uint8;
ALTER STREAM tbl UPDATE xi = c WHERE 1;
ALTER STREAM tbl DROP COLUMN c;
ALTER STREAM tbl ADD COLUMN c uint8;
ALTER STREAM tbl UPDATE c = xi WHERE 1;
ALTER STREAM tbl DROP COLUMN xi;

ALTER STREAM tbl ADD COLUMN xi float64;
ALTER STREAM tbl UPDATE xi = d WHERE 1;
ALTER STREAM tbl DROP COLUMN d;
ALTER STREAM tbl ADD COLUMN d float64;
ALTER STREAM tbl UPDATE d = xi WHERE 1;
ALTER STREAM tbl DROP COLUMN xi;

ALTER STREAM tbl ADD COLUMN xi uint32;
ALTER STREAM tbl UPDATE xi = e WHERE 1;
ALTER STREAM tbl DROP COLUMN e;
ALTER STREAM tbl ADD COLUMN e uint32;
ALTER STREAM tbl UPDATE e = xi WHERE 1;
ALTER STREAM tbl DROP COLUMN xi;

SELECT * FROM tbl FORMAT TabSeparatedWithNamesAndTypes;

DROP STREAM tbl;

-- Do the same thing again but with MODIFY COLUMN.
create stream tbl(a string, b uint32, c float64, d int64, e uint8) ENGINE=MergeTree ORDER BY tuple();
INSERT INTO tbl SELECT number, number * 2, number * 3, number * 4, number * 5 FROM system.numbers LIMIT 10;
ALTER STREAM tbl MODIFY COLUMN a int64, MODIFY COLUMN b string, MODIFY COLUMN c uint8, MODIFY COLUMN d float64, MODIFY COLUMN e uint32;
SELECT * FROM tbl FORMAT TabSeparatedWithNamesAndTypes;

DROP STREAM tbl;
