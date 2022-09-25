DROP STREAM IF EXISTS defaults;
create stream IF NOT EXISTS defaults
(
    vals string
) ;

insert into defaults values ('ba'), ('aa'), ('ba'), ('b'), ('ba'), ('aa');
select val < 1.5 and val > 1.459 from (select entropy(vals) as val from defaults);


DROP STREAM IF EXISTS defaults;
create stream IF NOT EXISTS defaults
(
     vals uint64
) ;
insert into defaults values (0), (0), (1), (0), (0), (0), (1), (2), (3), (5), (3), (1), (1), (4), (5), (2);
select val < 2.4 and val > 2.3393 from (select entropy(vals) as val from defaults);


DROP STREAM IF EXISTS defaults;
create stream IF NOT EXISTS defaults
(
    vals uint32
) ;
insert into defaults values (0), (0), (1), (0), (0), (0), (1), (2), (3), (5), (3), (1), (1), (4), (5), (2);
select val < 2.4 and val > 2.3393 from (select entropy(vals) as val from defaults);


DROP STREAM IF EXISTS defaults;
create stream IF NOT EXISTS defaults
(
    vals int32
) ;
insert into defaults values (0), (0), (-1), (0), (0), (0), (-1), (2), (3), (5), (3), (-1), (-1), (4), (5), (2);
select val < 2.4 and val > 2.3393 from (select entropy(vals) as val from defaults);


DROP STREAM IF EXISTS defaults;
create stream IF NOT EXISTS defaults
(
    vals datetime
) ;
insert into defaults values (to_datetime('2016-06-15 23:00:00')), (to_datetime('2016-06-15 23:00:00')), (to_datetime('2016-06-15 23:00:00')), (to_datetime('2016-06-15 23:00:00')), (to_datetime('2016-06-15 24:00:00')), (to_datetime('2016-06-15 24:00:00')), (to_datetime('2016-06-15 24:00:00')), (to_datetime('2017-06-15 24:00:00')), (to_datetime('2017-06-15 24:00:00')), (to_datetime('2018-06-15 24:00:00')), (to_datetime('2018-06-15 24:00:00')), (to_datetime('2019-06-15 24:00:00'));
select val < 2.189 and val > 2.1886 from (select entropy(vals) as val from defaults);

DROP STREAM defaults;
