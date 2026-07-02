--- LEFT ANTI JOIN: 3x3 storage combinations across 3 query modes.
--- Storage: Append (a), VersionedKV (v), MutableStream (m).
--- Query mode: table-table (tt), stream-table (st), stream-stream (ss).

drop view if exists 99922_st_aa;
drop view if exists 99922_st_av;
drop view if exists 99922_st_am;
drop view if exists 99922_st_va;
drop view if exists 99922_st_vv;
drop view if exists 99922_st_vm;
drop view if exists 99922_st_ma;
drop view if exists 99922_st_mv;
drop view if exists 99922_st_mm;
drop view if exists 99922_ss_aa;
drop view if exists 99922_ss_av;
drop view if exists 99922_ss_am;
drop view if exists 99922_ss_va;
drop view if exists 99922_ss_vv;
drop view if exists 99922_ss_vm;
drop view if exists 99922_ss_ma;
drop view if exists 99922_ss_mv;
drop view if exists 99922_ss_mm;
drop stream if exists 99922_left_append;
drop stream if exists 99922_right_append;
drop stream if exists 99922_left_vkv;
drop stream if exists 99922_right_vkv;
drop stream if exists 99922_left_mut;
drop stream if exists 99922_right_mut;
drop stream if exists 99922_st_la;
drop stream if exists 99922_st_lv;
drop stream if exists 99922_st_lm;
drop stream if exists 99922_ss_la;
drop stream if exists 99922_ss_ra;
drop stream if exists 99922_ss_lv;
drop stream if exists 99922_ss_rv;
drop stream if exists 99922_ss_lm;
drop stream if exists 99922_ss_rm;
drop view if exists 99922_mv_st_null;
drop view if exists 99922_mv_ss_null;
drop stream if exists 99922_st_null_out;
drop stream if exists 99922_ss_null_out;
drop stream if exists 99922_left_null;
drop stream if exists 99922_right_null;
drop stream if exists 99922_left_null_ss;
drop stream if exists 99922_right_null_ss;

create stream 99922_left_append(k int, v string) settings flush_threshold_count=1;
create stream 99922_right_append(k int, v string) settings flush_threshold_count=1;
create stream 99922_left_vkv(k int, v string) primary key k settings mode='versioned_kv', flush_threshold_count=1;
create stream 99922_right_vkv(k int, v string) primary key k settings mode='versioned_kv', flush_threshold_count=1;
create mutable stream 99922_left_mut(k int, v string) primary key k settings flush_rows=1;
create mutable stream 99922_right_mut(k int, v string) primary key k settings flush_rows=1;

insert into 99922_left_append(k, v) values (1, 'la1'), (2, 'la2'), (3, 'la3'), (4, 'la4');
insert into 99922_right_append(k, v) values (1, 'ra1'), (3, 'ra3');
insert into 99922_left_vkv(k, v) values (1, 'lv1'), (2, 'lv2'), (3, 'lv3'), (4, 'lv4');
insert into 99922_right_vkv(k, v) values (1, 'rv1'), (3, 'rv3');
insert into 99922_left_mut(k, v) values (1, 'lm1'), (2, 'lm2'), (3, 'lm3'), (4, 'lm4');
insert into 99922_right_mut(k, v) values (1, 'rm1'), (3, 'rm3');
select sleep(1) format Null;

--- ===== table-table: both sides snapshot via table() =====
select 'tt append left anti append';
select k from table(99922_left_append) l left anti join table(99922_right_append) r on l.k = r.k order by k settings query_mode='table';
select 'tt append anti vkv';
select k from table(99922_left_append) l anti join table(99922_right_vkv) r on l.k = r.k order by k settings query_mode='table';
select 'tt append left anti mut';
select k from table(99922_left_append) l left anti join table(99922_right_mut) r on l.k = r.k order by k settings query_mode='table';
select 'tt vkv anti append';
select k from table(99922_left_vkv) l anti join table(99922_right_append) r on l.k = r.k order by k settings query_mode='table';
select 'tt vkv left anti vkv';
select k from table(99922_left_vkv) l left anti join table(99922_right_vkv) r on l.k = r.k order by k settings query_mode='table';
select 'tt vkv anti mut';
select k from table(99922_left_vkv) l anti join table(99922_right_mut) r on l.k = r.k order by k settings query_mode='table';
select 'tt mut left anti append';
select k from table(99922_left_mut) l left anti join table(99922_right_append) r on l.k = r.k order by k settings query_mode='table';
select 'tt mut anti vkv';
select k from table(99922_left_mut) l anti join table(99922_right_vkv) r on l.k = r.k order by k settings query_mode='table';
select 'tt mut left anti mut';
select k from table(99922_left_mut) l left anti join table(99922_right_mut) r on l.k = r.k order by k settings query_mode='table';

--- ===== stream-table: streaming left, right = table() snapshot =====
--- Right streams already populated above; their snapshot is captured at MV creation.
create stream 99922_st_la(k int, v string) settings flush_threshold_count=1;
create stream 99922_st_lv(k int, v string) primary key k settings mode='versioned_kv', flush_threshold_count=1;
create mutable stream 99922_st_lm(k int, v string) primary key k settings flush_rows=1;

create materialized view 99922_st_aa as select l.k from 99922_st_la l left anti join table(99922_right_append) r on l.k = r.k storage_settings flush_threshold_count=1;
create materialized view 99922_st_av as select l.k from 99922_st_la l left anti join table(99922_right_vkv) r on l.k = r.k storage_settings flush_threshold_count=1;
create materialized view 99922_st_am as select l.k from 99922_st_la l left anti join table(99922_right_mut) r on l.k = r.k storage_settings flush_threshold_count=1;
create materialized view 99922_st_va as select l.k from 99922_st_lv l left anti join table(99922_right_append) r on l.k = r.k storage_settings flush_threshold_count=1;
create materialized view 99922_st_vv as select l.k from 99922_st_lv l left anti join table(99922_right_vkv) r on l.k = r.k storage_settings flush_threshold_count=1;
create materialized view 99922_st_vm as select l.k from 99922_st_lv l left anti join table(99922_right_mut) r on l.k = r.k storage_settings flush_threshold_count=1;
create materialized view 99922_st_ma as select l.k from 99922_st_lm l left anti join table(99922_right_append) r on l.k = r.k storage_settings flush_threshold_count=1;
create materialized view 99922_st_mv as select l.k from 99922_st_lm l left anti join table(99922_right_vkv) r on l.k = r.k storage_settings flush_threshold_count=1;
create materialized view 99922_st_mm as select l.k from 99922_st_lm l left anti join table(99922_right_mut) r on l.k = r.k storage_settings flush_threshold_count=1;

insert into 99922_st_la(k, v) values (1, 'la1'), (2, 'la2'), (3, 'la3'), (4, 'la4');
insert into 99922_st_lv(k, v) values (1, 'lv1'), (2, 'lv2'), (3, 'lv3'), (4, 'lv4');
insert into 99922_st_lm(k, v) values (1, 'lm1'), (2, 'lm2'), (3, 'lm3'), (4, 'lm4');
select sleep(1) format Null;
select sleep(1) format Null;

select 'st append left anti append';
select k from table(99922_st_aa) order by k settings query_mode='table';
select 'st append left anti vkv';
select k from table(99922_st_av) order by k settings query_mode='table';
select 'st append left anti mut';
select k from table(99922_st_am) order by k settings query_mode='table';
select 'st vkv left anti append';
select k from table(99922_st_va) order by k settings query_mode='table';
select 'st vkv left anti vkv';
select k from table(99922_st_vv) order by k settings query_mode='table';
select 'st vkv left anti mut';
select k from table(99922_st_vm) order by k settings query_mode='table';
select 'st mut left anti append';
select k from table(99922_st_ma) order by k settings query_mode='table';
select 'st mut left anti vkv';
select k from table(99922_st_mv) order by k settings query_mode='table';
select 'st mut left anti mut';
select k from table(99922_st_mm) order by k settings query_mode='table';

--- ===== stream-stream: both sides streaming via MV =====
create stream 99922_ss_la(k int, v string) settings flush_threshold_count=1;
create stream 99922_ss_ra(k int, v string) settings flush_threshold_count=1;
create stream 99922_ss_lv(k int, v string) primary key k settings mode='versioned_kv', flush_threshold_count=1;
create stream 99922_ss_rv(k int, v string) primary key k settings mode='versioned_kv', flush_threshold_count=1;
create mutable stream 99922_ss_lm(k int, v string) primary key k settings flush_rows=1;
create mutable stream 99922_ss_rm(k int, v string) primary key k settings flush_rows=1;

create materialized view 99922_ss_aa as select l.k from 99922_ss_la l left anti join 99922_ss_ra r on l.k = r.k storage_settings flush_threshold_count=1;
create materialized view 99922_ss_av as select l.k from 99922_ss_la l left anti join 99922_ss_rv r on l.k = r.k storage_settings flush_threshold_count=1;
create materialized view 99922_ss_am as select l.k from 99922_ss_la l left anti join 99922_ss_rm r on l.k = r.k storage_settings flush_threshold_count=1;
create materialized view 99922_ss_va as select l.k from 99922_ss_lv l left anti join 99922_ss_ra r on l.k = r.k storage_settings flush_threshold_count=1;
create materialized view 99922_ss_vv as select l.k from 99922_ss_lv l left anti join 99922_ss_rv r on l.k = r.k storage_settings flush_threshold_count=1;
create materialized view 99922_ss_vm as select l.k from 99922_ss_lv l left anti join 99922_ss_rm r on l.k = r.k storage_settings flush_threshold_count=1;
create materialized view 99922_ss_ma as select l.k from 99922_ss_lm l left anti join 99922_ss_ra r on l.k = r.k storage_settings flush_threshold_count=1;
create materialized view 99922_ss_mv as select l.k from 99922_ss_lm l left anti join 99922_ss_rv r on l.k = r.k storage_settings flush_threshold_count=1;
create materialized view 99922_ss_mm as select l.k from 99922_ss_lm l left anti join 99922_ss_rm r on l.k = r.k storage_settings flush_threshold_count=1;

insert into 99922_ss_ra(k, v) values (1, 'ra1'), (3, 'ra3');
insert into 99922_ss_rv(k, v) values (1, 'rv1'), (3, 'rv3');
insert into 99922_ss_rm(k, v) values (1, 'rm1'), (3, 'rm3');
select sleep(1) format Null;
insert into 99922_ss_la(k, v) values (1, 'la1'), (2, 'la2'), (3, 'la3'), (4, 'la4');
insert into 99922_ss_lv(k, v) values (1, 'lv1'), (2, 'lv2'), (3, 'lv3'), (4, 'lv4');
insert into 99922_ss_lm(k, v) values (1, 'lm1'), (2, 'lm2'), (3, 'lm3'), (4, 'lm4');
select sleep(1) format Null;
select sleep(1) format Null;

select 'ss append left anti append';
select k from table(99922_ss_aa) order by k settings query_mode='table';
select 'ss append left anti vkv';
select k from table(99922_ss_av) order by k settings query_mode='table';
select 'ss append left anti mut';
select k from table(99922_ss_am) order by k settings query_mode='table';
select 'ss vkv left anti append';
select k from table(99922_ss_va) order by k settings query_mode='table';
select 'ss vkv left anti vkv';
select k from table(99922_ss_vv) order by k settings query_mode='table';
select 'ss vkv left anti mut';
select k from table(99922_ss_vm) order by k settings query_mode='table';
select 'ss mut left anti append';
select k from table(99922_ss_ma) order by k settings query_mode='table';
select 'ss mut left anti vkv';
select k from table(99922_ss_mv) order by k settings query_mode='table';
select 'ss mut left anti mut';
select k from table(99922_ss_mm) order by k settings query_mode='table';

--- ===== EXPLAIN PIPELINE: validate planner accepts ANTI for all 9 storage combos =====
select 'plan append left anti append';
explain pipeline select l.k from 99922_left_append l left anti join 99922_right_append r on l.k = r.k;
select 'plan append anti vkv';
explain pipeline select l.k from 99922_left_append l anti join 99922_right_vkv r on l.k = r.k;
select 'plan append left anti mut';
explain pipeline select l.k from 99922_left_append l left anti join 99922_right_mut r on l.k = r.k;
select 'plan vkv anti append';
explain pipeline select l.k from 99922_left_vkv l anti join 99922_right_append r on l.k = r.k;
select 'plan vkv left anti vkv';
explain pipeline select l.k from 99922_left_vkv l left anti join 99922_right_vkv r on l.k = r.k;
select 'plan vkv anti mut';
explain pipeline select l.k from 99922_left_vkv l anti join 99922_right_mut r on l.k = r.k;
select 'plan mut left anti append';
explain pipeline select l.k from 99922_left_mut l left anti join 99922_right_append r on l.k = r.k;
select 'plan mut anti vkv';
explain pipeline select l.k from 99922_left_mut l anti join 99922_right_vkv r on l.k = r.k;
select 'plan mut left anti mut';
explain pipeline select l.k from 99922_left_mut l left anti join 99922_right_mut r on l.k = r.k;

--- ===== NULL key handling: NULL-keyed left rows are dropped from LEFT ANTI output across all 3 query modes =====
create stream 99922_left_null(k nullable(int), v string) settings flush_threshold_count=1;
create stream 99922_right_null(k nullable(int), v string) settings flush_threshold_count=1;
insert into 99922_left_null(k, v) values (1, 'l1'), (2, 'l2'), (null, 'lnull');
insert into 99922_right_null(k, v) values (1, 'r1'), (null, 'rnull');
select sleep(1) format Null;
select 'tt null';
select k, v from table(99922_left_null) l left anti join table(99922_right_null) r on l.k = r.k order by v settings query_mode='table';

--- stream-table NULL: streaming left side with NULL keys, table snapshot right side
create stream 99922_st_null_out(k nullable(int), v string) settings flush_threshold_count=1;
create materialized view 99922_mv_st_null into 99922_st_null_out as
  select l.k as k, l.v as v from 99922_left_null l left anti join table(99922_right_null) r on l.k = r.k;
insert into 99922_left_null(k, v) values (3, 'l3'), (null, 'lnull2');
select sleep(1) format Null;
select 'st null';
select k, v from table(99922_st_null_out) order by v;

--- stream-stream NULL: both sides streaming with NULL keys
create stream 99922_left_null_ss(k nullable(int), v string) settings flush_threshold_count=1;
create stream 99922_right_null_ss(k nullable(int), v string) settings flush_threshold_count=1;
create stream 99922_ss_null_out(k nullable(int), v string) settings flush_threshold_count=1;
create materialized view 99922_mv_ss_null into 99922_ss_null_out as
  select l.k as k, l.v as v from 99922_left_null_ss l left anti join 99922_right_null_ss r on l.k = r.k;
insert into 99922_right_null_ss(k, v) values (1, 'r1'), (null, 'rnull');
select sleep(1) format Null;
insert into 99922_left_null_ss(k, v) values (1, 'l1'), (2, 'l2'), (null, 'lnull');
select sleep(1) format Null;
select 'ss null';
select k, v from table(99922_ss_null_out) order by v;

drop view if exists 99922_st_aa;
drop view if exists 99922_st_av;
drop view if exists 99922_st_am;
drop view if exists 99922_st_va;
drop view if exists 99922_st_vv;
drop view if exists 99922_st_vm;
drop view if exists 99922_st_ma;
drop view if exists 99922_st_mv;
drop view if exists 99922_st_mm;
drop view if exists 99922_ss_aa;
drop view if exists 99922_ss_av;
drop view if exists 99922_ss_am;
drop view if exists 99922_ss_va;
drop view if exists 99922_ss_vv;
drop view if exists 99922_ss_vm;
drop view if exists 99922_ss_ma;
drop view if exists 99922_ss_mv;
drop view if exists 99922_ss_mm;
drop stream if exists 99922_st_la;
drop stream if exists 99922_st_lv;
drop stream if exists 99922_st_lm;
drop stream if exists 99922_ss_la;
drop stream if exists 99922_ss_ra;
drop stream if exists 99922_ss_lv;
drop stream if exists 99922_ss_rv;
drop stream if exists 99922_ss_lm;
drop stream if exists 99922_ss_rm;
drop stream if exists 99922_left_append;
drop stream if exists 99922_right_append;
drop stream if exists 99922_left_vkv;
drop stream if exists 99922_right_vkv;
drop stream if exists 99922_left_mut;
drop stream if exists 99922_right_mut;
drop view if exists 99922_mv_st_null;
drop view if exists 99922_mv_ss_null;
drop stream if exists 99922_st_null_out;
drop stream if exists 99922_ss_null_out;
drop stream if exists 99922_left_null;
drop stream if exists 99922_right_null;
drop stream if exists 99922_left_null_ss;
drop stream if exists 99922_right_null_ss;
