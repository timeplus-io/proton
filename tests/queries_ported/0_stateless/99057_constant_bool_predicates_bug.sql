drop stream if exists 99057_stream;
create stream 99057_stream(id int, value int);
select sleep(1) format Null;

insert into 99057_stream(id, value) values(1, 1);
select sleep(2) format Null;

select id, value from table(99057_stream) where 1=2;
select id, value from table(99057_stream) where 1=1;

drop stream 99057_stream;
