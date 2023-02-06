drop stream if exists tab;

create stream tab (id uint32, haystack string, pattern string) engine = MergeTree() order by id;
insert into tab values (1, 'aaaxxxaa\0xxx', 'x');

select count_substrings('aaaxxxaa\0xxx', pattern) from tab where id = 1;
select count_substrings_case_insensitive('aaaxxxaa\0xxx', pattern) from tab where id = 1;
select count_substrings_case_insensitive_utf8('aaaxxxaa\0xxx', pattern) from tab where id = 1;

select count_substrings(haystack, pattern) from tab where id = 1;
select count_substrings_case_insensitive(haystack, pattern) from tab where id = 1;
select count_substrings_case_insensitive_utf8(haystack, pattern) from tab where id = 1;

insert into tab values (2, 'aaaaa\0x', 'x');

select position('aaaaa\0x', pattern) from tab where id = 2;
select position_case_insensitive('aaaaa\0x', pattern) from tab where id = 2;
select position_case_insensitive_utf8('aaaaa\0x', pattern) from tab where id = 2;

select position(haystack, pattern) from tab where id = 2;
select position_case_insensitive(haystack, pattern) from tab where id = 2;
select position_case_insensitive_utf8(haystack, pattern) from tab where id = 2;

drop stream if exists tab;
