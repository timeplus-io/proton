DROP STREAM IF EXISTS enum_as_num;

CREATE STREAM enum_as_num (
    Id int32,
    Value enum('a' = 1, '3' = 2, 'b' = 3)
) ENGINE=Memory();

INSERT INTO enum_as_num FORMAT TSV 1	1
INSERT INTO enum_as_num FORMAT TSV 2	2
INSERT INTO enum_as_num FORMAT TSV 3	3
INSERT INTO enum_as_num FORMAT TSV 4	a
INSERT INTO enum_as_num FORMAT TSV 5	b

INSERT INTO enum_as_num FORMAT CSV 6,1
INSERT INTO enum_as_num FORMAT CSV 7,2
INSERT INTO enum_as_num FORMAT CSV 8,3
INSERT INTO enum_as_num FORMAT CSV 9,a
INSERT INTO enum_as_num FORMAT CSV 10,b

SELECT * FROM enum_as_num ORDER BY Id;


DROP STREAM IF EXISTS enum_as_num;
