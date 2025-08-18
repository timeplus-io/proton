drop view if exists 99100_param_view;
drop stream if exists 99100_github_events;

create view 99100_param_view as select * from 99100_github_events limit {limit:int8};

SET query_mode='table';

select * from 99100_param_view(limit=2); -- { serverError UNKNOWN_STREAM }
select * from 99100_param_view; -- { serverError UNKNOWN_STREAM }

create stream 99100_github_events(id int);


select * from 99100_param_view(limit=2);
select * from 99100_param_view; -- { serverError UNKNOWN_QUERY_PARAMETER }


drop view if exists 99100_param_view;
drop stream if exists 99100_github_events;
