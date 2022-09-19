SET readonly = 2;

CREATE TEMPORARY STREAM readonly00542 (
    ID int
) Engine = Memory;

INSERT INTO readonly00542 (ID)
    VALUES (1), (2), (3), (4), (5);

SELECT ID FROM readonly00542 ORDER BY ID;

INSERT INTO readonly00542 (ID)
    SELECT CAST(number * 10 AS int) FROM system.numbers LIMIT 10;

SELECT '---';

SELECT ID FROM readonly00542 ORDER BY ID;

DROP TEMPORARY TABLE readonly00542;
