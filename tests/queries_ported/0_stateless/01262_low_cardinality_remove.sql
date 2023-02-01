DROP STREAM IF EXISTS testView;
DROP STREAM IF EXISTS testTable;

CREATE STREAM IF NOT EXISTS testTable (
 A low_cardinality(string), -- like voter
 B int64
) ENGINE MergeTree()
ORDER BY (A);

INSERT INTO testTable VALUES ('A', 1),('B',2),('C',3);

CREATE VIEW testView AS 
SELECT
 A as ALow, -- like account
 B
FROM
   testTable;

SELECT CAST(ALow, 'string') AS AStr
FROM testView
GROUP BY AStr ORDER BY AStr;

DROP STREAM testTable;

CREATE STREAM IF NOT EXISTS testTable (
 A string, -- like voter
 B int64
) ENGINE MergeTree()
ORDER BY (A);

SELECT CAST(ALow, 'string') AS AStr
FROM testView
GROUP BY AStr ORDER BY AStr;

INSERT INTO testTable VALUES ('A', 1),('B',2),('C',3);

SELECT CAST(ALow, 'string') AS AStr
FROM testView
GROUP BY AStr ORDER BY AStr;

DROP STREAM IF EXISTS testView;
DROP STREAM IF EXISTS testTable;
