-- Tags: no-fasttest

set output_format_write_statistics=0;

select 'CSV';
select 'formatRow';
select formatRow('CSV', number, good) from (select number, 'good' as good from numbers(3));
select 'formatRowNoNewline';
select formatRowNoNewline('CSV', number, good) from (select number, 'good' as good from numbers(3));

select 'TSV';
select 'formatRow';
select formatRow('TSV', number, good) from (select number, 'good' as good from numbers(3));
select 'formatRowNoNewline';
select formatRowNoNewline('TSV', number, good) from (select number, 'good' as good from numbers(3));

select 'JSONEachRow';
select 'formatRow';
select formatRow('JSONEachRow', number, good) from (select number, 'good' as good from numbers(3));
select 'formatRowNoNewline';
select formatRowNoNewline('JSONEachRow', number, good) from (select number, 'good' as good from numbers(3));

select 'JSONCompactEachRow';
select 'formatRow';
select formatRow('JSONCompactEachRow', number, good) from (select number, 'good' as good from numbers(3));
select 'formatRowNoNewline';
select formatRowNoNewline('JSONCompactEachRow', number, good) from (select number, 'good' as good from numbers(3));

select 'TSKV';
select 'formatRow';
select formatRow('TSKV', number, good) from (select number, 'good' as good from numbers(3));
select 'formatRowNoNewline';
select formatRowNoNewline('TSKV', number, good) from (select number, 'good' as good from numbers(3));

select 'XML';
select 'formatRow';
select formatRow('XML', number, good) from (select number, 'good' as good from numbers(3));
select 'formatRowNoNewline';
select formatRowNoNewline('XML', number, good) from (select number, 'good' as good from numbers(3));

select 'Markdown';
select 'formatRow';
select formatRow('Markdown', number, good) from (select number, 'good' as good from numbers(3));
select 'formatRowNoNewline';
select formatRowNoNewline('Markdown', number, good) from (select number, 'good' as good from numbers(3));

select 'CustomSeparated';
select 'formatRow';
select formatRow('CustomSeparated', number, good) from (select number, 'good' as good from numbers(3));
select 'formatRowNoNewline';
select formatRowNoNewline('CustomSeparated', number, good) from (select number, 'good' as good from numbers(3));

select 'SQLInsert';
select 'formatRow';
select formatRow('SQLInsert', number, good) from (select number, 'good' as good from numbers(3));
select 'formatRowNoNewline';
select formatRowNoNewline('SQLInsert', number, good) from (select number, 'good' as good from numbers(3));

select 'Vertical';
select 'formatRow';
select formatRow('Vertical', number, good) from (select number, 'good' as good from numbers(3));
select 'formatRowNoNewline';
select formatRowNoNewline('Vertical', number, good) from (select number, 'good' as good from numbers(3));

select 'JSON';
select 'formatRow';
select formatRow('JSON', number, good) from (select number, 'good' as good from numbers(3));
select 'formatRowNoNewline';
select formatRowNoNewline('JSON', number, good) from (select number, 'good' as good from numbers(3));

select 'JSONCompact';
select 'formatRow';
select formatRow('JSONCompact', number, good) from (select number, 'good' as good from numbers(3));
select 'formatRowNoNewline';
select formatRowNoNewline('JSONCompact', number, good) from (select number, 'good' as good from numbers(3));

select 'Values';
select 'formatRow';
select formatRow('Values', number, good) from (select number, 'good' as good from numbers(3));
select 'formatRowNoNewline';
select formatRowNoNewline('Values', number, good) from (select number, 'good' as good from numbers(3));

-- unknown format
select formatRow('aaa', *) from numbers(3); -- { serverError 73 }

