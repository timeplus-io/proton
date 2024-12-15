CREATE STREAM IF NOT EXISTS 99010_udf_types(`f32` float);

CREATE AGGREGATE FUNCTION test_sec_large_99010(value float32) RETURNS float32 LANGUAGE JAVASCRIPT AS $$
                  {
                    initialize: function() {
                       this.max = -1.0;
                       this.sec = -1.0
                    },
                    process: function(values) {
                      for (let i = 0; i < values.length; i++) {
                        if (values[i] > this.max) {
                          this.sec = this.max;
                          this.max = values[i]
                        }
                        if (values[i] < this.max && values[i] > this.sec)
                          this.sec = values[i];
                      }
                    },
                          finalize: function() {
                          return this.sec
                          },
                          serialize: function() {
                          let s = {
                          'max': this.max,
                          'sec': this.sec
                          };
                      return JSON.stringify(s)
                    },
                      deserialize: function(state_str) {
                                                         let s = JSON.parse(state_str);
                                                         this.max = s['max'];
                                                         this.sec = s['sec']
                      },
                      merge: function(state_str) {
                                                   let s = JSON.parse(state_str);
                                                   if (s['sec'] >= this.max) {
                                                   this.max = s['max'];
                                                   this.sec = s['sec']
                                                   } else if (s['max'] >= this.max) {
                                                   this.sec = this.max;
                                                   this.max = s['max']
                                                   } else if (s['max'] > this.sec) {
                                                   this.sec = s['max']
                                                   }
                                                   }
                      }
              $$;

select sleep(1) FORMAT Null;
insert into 99010_udf_types(f32) values(2.0);
select sleep(1) FORMAT Null;
insert into 99010_udf_types(f32) values(2.0);
select sleep(1) FORMAT Null;
insert into 99010_udf_types(f32) values(2.0);
select sleep(1) FORMAT Null;


select test_sec_large_99010(f32) from table(99010_udf_types) settings javascript_max_memory_bytes=2;  --- { serverError UDF_MEMORY_THRESHOLD_EXCEEDED }

with acc as (select test_sec_large_99010(f32) as res from table(99010_udf_types) settings javascript_max_memory_bytes=2) select min(res) as re from acc; --- { serverError UDF_MEMORY_THRESHOLD_EXCEEDED }



DROP STREAM 99010_udf_types;
DROP FUNCTION test_sec_large_99010;
