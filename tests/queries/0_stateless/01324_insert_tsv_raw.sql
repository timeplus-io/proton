SET query_mode = 'table';
drop stream if exists tsv_raw;
create stream tsv_raw (strval string, intval int64, b1 string, b2 string, b3 string, b4 string) engine = Memory;
insert into tsv_raw format TSVRaw "a 	1	\	\\	"\""	"\\""
;

select * from tsv_raw format TSVRaw;
select * from tsv_raw format JSONCompactEachRow;
drop stream tsv_raw;
