DROP STREAM IF EXISTS t1;
DROP STREAM IF EXISTS t2;
DROP STREAM IF EXISTS t3;
DROP STREAM IF EXISTS t4;

create stream t1 (id string, name string, value uint32)
ENGINE = Join(ANY, LEFT, id)
SETTINGS join_use_nulls = 1;

create stream t2 (id string, name string, value uint32)
ENGINE = Join(ANY, LEFT, id)
SETTINGS join_use_nulls = 0;

create stream t3 (id nullable(string), name string, value uint32)
ENGINE = Join(ANY, LEFT, id)
SETTINGS join_use_nulls = 1;

create stream t4 (id string, name nullable(string), value uint32)
ENGINE = Join(ANY, LEFT, id)
SETTINGS join_use_nulls = 0;

insert into t1 values('1', 's', 1);
insert into t2 values('2', 's', 2);
insert into t3 values('3', 's', 3);
insert into t4 values('4', 's', 4);

select *, to_type_name(id), to_type_name(name) from t1;
select *, to_type_name(id), to_type_name(name) from t2;
select *, to_type_name(id), to_type_name(name) from t3;
select *, to_type_name(id), to_type_name(name) from t4;

SET join_use_nulls = 1;

select *, to_type_name(id), to_type_name(name) from t1;
select *, to_type_name(id), to_type_name(name) from t2;
select *, to_type_name(id), to_type_name(name) from t3;
select *, to_type_name(id), to_type_name(name) from t4;

DROP STREAM t1;
DROP STREAM t2;
DROP STREAM t3;
DROP STREAM t4;
