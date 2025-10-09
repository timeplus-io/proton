set allow_experimental_variant_type=1;

drop stream if exists test;
create stream test (v variant(string, uint64));
select sleep(1) format Null;
insert into test(v) values (42), ('Hello'), (NULL);
select sleep(3) format Null;

select v from table(test) where v = 'Hello';
select v from table(test) where v = 42; -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
select v from table(test) where v = 42::uint64::variant(string, uint64);

drop stream test;

