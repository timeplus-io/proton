drop stream if exists sequence_test;

create stream sequence_test (time uint32, data uint8) engine=Memory;

insert into sequence_test (time, data) values (0,0),(1,0),(2,0),(3,0),(4,1),(5,2),(6,0),(7,0),(8,0),(9,0),(10,1),(11,1);

select 1 = sequenceMatch('')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 1 = sequenceMatch('.')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 1 = sequenceMatch('.*')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 1 = sequenceMatch('(?1)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 1 = sequenceMatch('(?2)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 1 = sequenceMatch('(?3)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 0 = sequenceMatch('(?4)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 1 = sequenceMatch('(?1)(?1)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 1 = sequenceMatch('(?1)(?1)(?1)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 1 = sequenceMatch('(?1)(?1)(?1)(?1)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 0 = sequenceMatch('(?1)(?1)(?1)(?1)(?1)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 1 = sequenceMatch('(?1)(?1)(?1)(?1)(?2)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 1 = sequenceMatch('(?1)(?t>10)(?2)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 0 = sequenceMatch('(?1)(?t>11)(?2)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 1 = sequenceMatch('(?1)(?t<11)(?2)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 1 = sequenceMatch('(?1)(?t<3)(?3)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 1 = sequenceMatch('(?1)(?t<=2)(?3)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 0 = sequenceMatch('(?1)(?t<2)(?3)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 1 = sequenceMatch('(?2)(?t>=7)(?2)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 0 = sequenceMatch('(?2)(?t>7)(?2)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 1 = sequenceMatch('(?2)(?3)(?1)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 0 = sequenceMatch('(?1)(?t==2)(?2)')(time, data = 1, data = 2) from sequence_test;
select 1 = sequenceMatch('(?1)(?t==1)(?2)')(time, data = 1, data = 2) from sequence_test;

select count() = sequenceCount('')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select count() = sequenceCount('.')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select count() = sequenceCount('.*')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 8 = sequenceCount('(?1)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 3 = sequenceCount('(?2)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 1 = sequenceCount('(?3)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 0 = sequenceCount('(?4)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 4 = sequenceCount('(?1)(?1)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 2 = sequenceCount('(?1)(?1)(?1)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 2 = sequenceCount('(?1)(?1)(?1)(?1)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 0 = sequenceCount('(?1)(?1)(?1)(?1)(?1)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 2 = sequenceCount('(?1)(?1)(?1)(?1)(?2)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 1 = sequenceCount('(?1)(?t>10)(?2)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 0 = sequenceCount('(?1)(?t>11)(?2)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 2 = sequenceCount('(?1)(?t<11)(?2)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 1 = sequenceCount('(?1)(?t<3)(?3)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 1 = sequenceCount('(?1)(?t<=2)(?3)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 0 = sequenceCount('(?1)(?t<2)(?3)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 1 = sequenceCount('(?2)(?t>=7)(?2)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 0 = sequenceCount('(?2)(?t>7)(?2)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 1 = sequenceCount('(?2)(?3)(?1)')(time, data = 0, data = 1, data = 2, data = 3) from sequence_test;
select 0 = sequenceCount('(?1)(?t==2)(?2)')(time, data = 1, data = 2) from sequence_test;
select 1 = sequenceCount('(?1)(?t==1)(?2)')(time, data = 1, data = 2) from sequence_test;

drop stream sequence_test;
