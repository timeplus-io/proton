drop stream if exists ax;
drop stream if exists bx;

create stream ax (A int64, B int64) Engine = Memory;
create stream bx (A int64) Engine = Memory;

insert into ax values (1, 1), (2, 1);
insert into bx values (2), (4);

select * from bx, ax where ax.A = bx.A and ax.B in (1,2);

drop stream ax;
drop stream bx;
