select * from system.one cross join system.one; -- { serverError 352 }
select * from system.one cross join system.one as r;
select * from system.one as l cross join system.one;
select * from system.one left join system.one using dummy;
select dummy from system.one left join system.one using dummy;

USE system;

SELECT dummy FROM one AS A JOIN one ON A.dummy = one.dummy;
SELECT dummy FROM one JOIN one AS A ON A.dummy = one.dummy;
SELECT dummy FROM one as l JOIN one as r ON dummy = r.dummy; -- { serverError 352 }
SELECT dummy FROM one as l JOIN one as r ON l.dummy = dummy; -- { serverError 352 }
SELECT dummy FROM one as l JOIN one as r ON one.dummy = r.dummy; -- { serverError 352 }
SELECT dummy FROM one as l JOIN one as r ON l.dummy = one.dummy; -- { serverError 352 }

SELECT * from one
JOIN one as A ON one.dummy = A.dummy
JOIN one as B ON one.dummy = B.dummy
FORMAT PrettyCompact;

SELECT * from one as A
JOIN system.one as one ON A.dummy = one.dummy
JOIN system.one as two ON A.dummy = two.dummy
FORMAT PrettyCompact;

-- SELECT one.dummy FROM one AS A FULL JOIN (SELECT 0 AS dymmy) AS one USING dummy;
SELECT one.dummy FROM one AS A JOIN (SELECT 0 AS dummy) as B USING dummy;
