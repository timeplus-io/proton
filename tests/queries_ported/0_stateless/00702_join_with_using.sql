DROP STREAM IF EXISTS using1;
DROP STREAM IF EXISTS using2;

CREATE STREAM using1(a uint8, b uint8) ENGINE=Memory;
CREATE STREAM using2(a uint8, b uint8) ENGINE=Memory;

INSERT INTO using1(a, b) VALUES (1, 1) (2, 2) (3, 3);
INSERT INTO using2(a, b) VALUES (4, 4) (2, 2) (3, 3);
SELECT SLEEP(1);

SELECT * FROM using1 ALL LEFT JOIN (SELECT * FROM using2) as js2 USING (a, b) ORDER BY a;

DROP STREAM using1;
DROP STREAM using2;

--

DROP STREAM if exists persons;
DROP STREAM if exists children;

CREATE STREAM persons (id string, name string) engine MergeTree order by id;
CREATE STREAM children (id string, childName string) engine MergeTree order by id;

insert into persons (id, name)
values ('1', 'John'), ('2', 'Jack'), ('3', 'Daniel'), ('4', 'James'), ('5', 'Amanda');

insert into children (id, childName)
values ('1', 'Robert'), ('1', 'Susan'), ('3', 'Sarah'), ('4', 'David'), ('4', 'Joseph'), ('5', 'Robert');

SELECT SLEEP(1);

select * from persons all inner join children using id order by id, name, childName;
select * from persons all inner join (select * from children) as j using id order by id, name, childName;
select * from (select * from persons) as s all inner join (select * from children ) as j using id order by id, name, childName;
--
set joined_subquery_requires_alias = 0;
select * from persons all inner join (select * from children) using id order by id, name, childName;
select * from (select * from persons) all inner join (select * from children) using id order by id, name, childName;
select * from (select * from persons) as s all inner join (select * from children) using id order by id, name, childName;

DROP STREAM persons;
DROP STREAM children;