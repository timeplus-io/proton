select * from table(numbers(100)); -- { serverError BAD_ARGUMENTS }
select * from table(system.processes); -- { serverError BAD_ARGUMENTS }
