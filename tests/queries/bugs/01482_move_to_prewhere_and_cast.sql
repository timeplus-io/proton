-- Tags: no-polymorphic-parts
-- Tag no-polymorphic-parts: bug, shoud be fixed

DROP STREAM IF EXISTS APPLICATION;
DROP STREAM IF EXISTS DATABASE_IO;

CREATE TABLE APPLICATION (
  `Name` LowCardinality(String),
  `Base` LowCardinality(String)
) ();

insert into table APPLICATION values ('ApplicationA', 'BaseA'), ('ApplicationB', 'BaseB') , ('ApplicationC', 'BaseC');

CREATE TABLE DATABASE_IO (
  `Application` LowCardinality(String),
  `Base` LowCardinality(String),
  `date` DateTime,
  `Ios` UInt32  ) 
ENGINE = MergeTree()
ORDER BY date;
  
insert into table DATABASE_IO  values ('AppA', 'BaseA', '2020-01-01 00:00:00', 1000);

SELECT `APPLICATION`.`Name` AS `App`,
       CAST(CAST(`DATABASE_IO`.`date` AS DATE) AS DATE) AS `date` 
FROM   `DATABASE_IO`
INNER 
JOIN   `APPLICATION` ON (`DATABASE_IO`.`Base` = `APPLICATION`.`Base`)
WHERE (
       CAST(CAST(`DATABASE_IO`.`date` AS DATE) AS TIMESTAMP) >= to_datetime('2020-01-01 00:00:00')
);

DROP STREAM APPLICATION;
DROP STREAM DATABASE_IO;
