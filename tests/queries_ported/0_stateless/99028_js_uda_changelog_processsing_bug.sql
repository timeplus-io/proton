DROP VIEW IF EXISTS 99028_udf_types_mv;
DROP STREAM IF EXISTS 99028_udf_types;
CREATE STREAM IF NOT EXISTS 99028_udf_types(`id` int, `value` float);

CREATE OR REPLACE AGGREGATE FUNCTION jsFunc_99028(value float) RETURNS float LANGUAGE JAVASCRIPT AS $$
{
  initialize: function() {
    this.any = 0;
  },
  process: function(values) {
    if (values.length > 0) {
      this.any = values[0];
    }
  },
  finalize: function() {
    return this.any;
  }
}
$$;

CREATE MATERIALIZED VIEW 99028_udf_types_mv AS select id, jsFunc_99028(value) as val from changelog(99028_udf_types, id) where id = 1 group by id emit periodic 1ms;
select sleep(2) format Null;

insert into 99028_udf_types(id, value) values(1, 1.1);
select sleep(1) format Null;
insert into 99028_udf_types(id, value) values(1, 2.2);

select sleep(3) format Null;
select id, val from table(99028_udf_types_mv) order by _tp_time asc;

DROP VIEW 99028_udf_types_mv;
DROP STREAM 99028_udf_types;
DROP FUNCTION jsFunc_99028;
