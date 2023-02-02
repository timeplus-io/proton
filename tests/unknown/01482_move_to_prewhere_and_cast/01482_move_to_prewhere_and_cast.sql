DROP STREAM IF EXISTS APPLICATION;
DROP STREAM IF EXISTS DATABASE_IO;

CREATE STREAM APPLICATION (
  `Name` low_cardinality(string),
  `Base` low_cardinality(string)
) ENGINE = Memory();

insert into APPLICATION(`Name`,`Base`) values ('ApplicationA', 'BaseA'), ('ApplicationB', 'BaseB') , ('ApplicationC', 'BaseC');

CREATE STREAM DATABASE_IO (
  `Application` low_cardinality(string),
  `Base` low_cardinality(string),
  `Date` DateTime,
  `Ios` uint32  ) 
ENGINE = MergeTree()
ORDER BY Date;
  
insert into DATABASE_IO(`Application`,`Base`, `Date`,`Ios`)  values ('AppA', 'BaseA', '2020-01-01 00:00:00', 1000);

SELECT `APPLICATION`.`Name` AS `App`,
       CAST(CAST(`DATABASE_IO`.`Date` AS DATE) AS DATE) AS `date`
FROM   `DATABASE_IO`
INNER
JOIN   `APPLICATION` ON (`DATABASE_IO`.`Base` = `APPLICATION`.`Base`)
WHERE (
       CAST(CAST(`DATABASE_IO`.`Date` AS DATE) AS TIMESTAMP) >= to_datetime('2020-01-01 00:00:00')
);

DROP STREAM APPLICATION;
DROP STREAM DATABASE_IO;
