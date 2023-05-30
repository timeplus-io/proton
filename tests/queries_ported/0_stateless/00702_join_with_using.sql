SET query_mode = 'table';
DROP STREAM IF EXISTS using1;
DROP STREAM IF EXISTS using2;

create stream using1(a uint8, b uint8) ENGINE=Memory;
create stream using2(a uint8, b uint8) ENGINE=Memory;

INSERT INTO using1 VALUES (1, 1) (2, 2) (3, 3);
INSERT INTO using2 VALUES (4, 4) (2, 2) (3, 3);

SELECT * FROM using1 ALL LEFT JOIN (SELECT * FROM using2) as js2 USING (a, a, a, b, b, b, a, a) ORDER BY a;

DROP STREAM using1;
DROP STREAM using2;

--

drop stream if exists persons;
drop stream if exists children;

create stream persons (id string, name string) engine MergeTree order by id;
create stream children (id string, childName string) engine MergeTree order by id;

insert into persons (id, name)
values ('1', 'John'), ('2', 'Jack'), ('3', 'Daniel'), ('4', 'James'), ('5', 'Amanda');

insert into children (id, childName)
values ('1', 'Robert'), ('1', 'Susan'), ('3', 'Sarah'), ('4', 'David'), ('4', 'Joseph'), ('5', 'Robert');

select * from persons all inner join children using id;
select * from persons all inner join (select * from children) as j using id;
select * from (select * from persons) as s all inner join (select * from children ) as j using id;
--
set joined_subquery_requires_alias = 0;
select * from persons all inner join (select * from children) using id;
select * from (select * from persons) all inner join (select * from children) using id;
select * from (select * from persons) as s all inner join (select * from children) using id;

drop stream persons;
drop stream children;
