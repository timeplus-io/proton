drop stream if exists data_01832;

-- Memory writes from the writeSuffix() and if it will be called twice two rows
-- will be written (since it does not reset the block).
create stream data_01832 (key int) Engine=Memory;
insert into data_01832 values (1);
select * from data_01832;

drop stream data_01832;
